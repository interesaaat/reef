// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Org.Apache.REEF.Network.Elastic.Task.Impl;
using System.Collections.Generic;
using System;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Exceptions;
using System.Threading;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.NetworkService;
using System.Collections.Concurrent;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal abstract class OneToNTopology : OperatorTopologyWithCommunication, ICheckpointingTopology
    {
        protected readonly CheckpointService _checkpointService;
        protected readonly ConcurrentDictionary<string, byte> _toRemove;

        protected readonly ManualResetEvent _topologyUpdateReceived;

        StreamingNetworkService<GroupCommunicationMessage> _networkService;

        protected OneToNTopology(
            string subscriptionName,
            string rootTaskId,
            ISet<int> children,
            bool piggyback,
            string taskId,
            int operatorId,
            int retry,
            int timeout,
            int disposeTimeout,
            CommunicationLayer commLayer,
            CheckpointService checkpointService,
            StreamingNetworkService<GroupCommunicationMessage> networkService) : base(taskId, rootTaskId, subscriptionName, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _networkService = networkService;
            _checkpointService = checkpointService;
            _toRemove = new ConcurrentDictionary<string, byte>();
            _topologyUpdateReceived = new ManualResetEvent(RootTaskId == taskId ? false : true);

            _commLayer.RegisterOperatorTopologyForTask(TaskId, this);
            _commLayer.RegisterOperatorTopologyForDriver(TaskId, this);

            foreach (var child in children)
            {
                var childTaskId = Utils.BuildTaskId(SubscriptionName, child);

                _children.TryAdd(child, childTaskId);
            }
        }

        internal bool IsSending
        {
            get { return !_sendQueue.IsEmpty; }
        }

        public ICheckpointState InternalCheckpoint { get; private set; }

        public void Checkpoint(ICheckpointableState state, int? iteration = null)
        {
            ICheckpointState checkpoint;

            switch (state.Level)
            {
                case CheckpointLevel.None:
                    break;
                case CheckpointLevel.EphemeralMaster:
                    if (TaskId == RootTaskId)
                    {
                        InternalCheckpoint = state.Checkpoint();
                    }
                    break;
                case CheckpointLevel.EphemeralAll:
                    checkpoint = state.Checkpoint();
                    InternalCheckpoint = checkpoint;
                    break;
                case CheckpointLevel.PersistentMemoryMaster:
                    if (TaskId == RootTaskId)
                    {
                        checkpoint = state.Checkpoint();
                        checkpoint.OperatorId = OperatorId;
                        checkpoint.SubscriptionName = SubscriptionName;
                        _checkpointService.Checkpoint(checkpoint);
                    }
                    break;
                case CheckpointLevel.PersistentMemoryAll:
                    checkpoint = state.Checkpoint();
                    checkpoint.OperatorId = OperatorId;
                    checkpoint.SubscriptionName = SubscriptionName;
                    _checkpointService.Checkpoint(checkpoint);
                    break;
                default:
                    throw new IllegalStateException("Checkpoint level not supported.");
            }
        }

        public bool GetCheckpoint(out ICheckpointState checkpoint, int iteration = -1)
        {
            if (InternalCheckpoint != null && (iteration == -1 || InternalCheckpoint.Iteration == iteration))
            {
                checkpoint = InternalCheckpoint;
                return true;
            }

            return _checkpointService.GetCheckpoint(out checkpoint, TaskId, SubscriptionName, OperatorId, iteration, false);
        }

        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                _commLayer.WaitForTaskRegistration(_children.Values.ToList(), cancellationSource, _toRemove);
            }
            catch (Exception e)
            {
                throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + TaskId, e);
            }

            _initialized = true;

            Send(cancellationSource);
        }

        public void WaitCompletionBeforeDisposing(CancellationTokenSource cancellationSource)
        {
            if (TaskId == RootTaskId)
            {
                foreach (var node in _children.Values)
                {
                    while (_commLayer.Lookup(node) == true && !cancellationSource.IsCancellationRequested)
                    {
                        Thread.Sleep(100);
                    }
                }   
            }
        }

        internal void TopologyUpdateRequest()
        {
            _commLayer.TopologyUpdateRequest(TaskId, OperatorId);
        }

        public override void OnNext(NsMessage<GroupCommunicationMessage> message)
        {
            if (_messageQueue.IsAddingCompleted)
            {
                throw new IllegalStateException("Trying to add messages to a closed non-empty queue");
            }

            _messageQueue.Add(message.Data);

            var topologyPayload = message.Data as DataMessageWithTopology;
            var updates = topologyPayload.TopologyUpdates;

            UpdateTopology(ref updates);
            topologyPayload.TopologyUpdates = updates;

            if (!_children.IsEmpty)
            {
                _sendQueue.Enqueue(message.Data);
            }

            if (_initialized)
            {
                Send(new CancellationTokenSource());
            }
        }

        internal override void OnFailureResponseMessageFromDriver(DriverMessagePayload value)
        {
            throw new NotImplementedException();
        }

        internal override void OnMessageFromDriver(DriverMessagePayload message)
        {
            if (message.PayloadType != DriverMessagePayloadType.Topology)
            {
                throw new IllegalStateException("Message not appropriate for OneToN topology");
            }

            var rmsg = message as TopologyMessagePayload;

            if (rmsg.ToRemove == true)
            {
                lock (_toRemove)
                {
                    foreach (var updates in rmsg.TopologyUpdates)
                    {
                        foreach (var node in updates.Children)
                        {
                            _toRemove.TryAdd(node, new byte());
                            _commLayer.RemoveConnection(node);
                        }
                    }
                }
            }
            else if (_sendQueue.Count > 0)
            {               
                if (_sendQueue.TryPeek(out GroupCommunicationMessage toSendmsg))
                {
                    var toSendmsgWithTop = toSendmsg as DataMessageWithTopology;
                    var updates = rmsg.TopologyUpdates;

                    UpdateTopology(ref updates);
                    toSendmsgWithTop.TopologyUpdates = updates;
                    
                    lock (_toRemove)
                    {
                        foreach (var taskId in _toRemove.Keys)
                        {
                            var id = Utils.GetTaskNum(taskId);
                            _children.TryRemove(id, out string val);
                        }
                        _toRemove.Clear();
                    }
                }

                _topologyUpdateReceived.Set();
            }
            else
            {
                Logger.Log(Level.Warning, "Received a topology update message from driver but sending queue is empty: ignoring");
            }
        }

        private void UpdateTopology(ref List<TopologyUpdate> updates)
        {
            TopologyUpdate toRemove = null;
            foreach (var update in updates)
            {
                if (update.Node == TaskId)
                {
                    toRemove = update;
                    foreach (var child in update.Children)
                    { 
                        if (!_toRemove.TryRemove(child, out byte value))
                        {
                            var id = Utils.GetTaskNum(child);
                            _children.TryAdd(id, child);
                        }
                    }
                }
            }

            if (toRemove != null)
            {
                updates.Remove(toRemove);
            }
        }
    }
}
