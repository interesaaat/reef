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

using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using System.Collections.Generic;
using Org.Apache.REEF.Common.Tasks;
using System;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Exceptions;
using System.Threading;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.NetworkService;
using System.Collections.Concurrent;
using System.Linq;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class BroadcastTopology : OperatorTopologyWithCommunication, ICheckpointingTopology
    {
        private readonly CheckpointService _checkpointService;
        private readonly ConcurrentDictionary<int, byte> _toRemove;

        private readonly ManualResetEvent _topologyUpdateReceived;

        StreamingNetworkService<GroupCommunicationMessage> _networkService;

        [Inject]
        private BroadcastTopology(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscription,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(OperatorId))] int operatorId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int disposeTimeout,
            CommunicationLayer commLayer,
            CheckpointService checkpointService,
            StreamingNetworkService<GroupCommunicationMessage> networkService) : base(taskId, rootId, subscription, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _networkService = networkService;
            _checkpointService = checkpointService;
            _toRemove = new ConcurrentDictionary<int, byte>();
            _topologyUpdateReceived = new ManualResetEvent(_rootTaskId == taskId ? false : true);

            _commLayer.RegisterOperatorTopologyForTask(_taskId, this);
            _commLayer.RegisterOperatorTopologyForDriver(_taskId, this);

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
                    if (_taskId == _rootTaskId)
                    {
                        InternalCheckpoint = state.Checkpoint();
                    }
                    break;
                case CheckpointLevel.EphemeralAll:
                    checkpoint = state.Checkpoint();
                    InternalCheckpoint = checkpoint;
                    break;
                case CheckpointLevel.PersistentMemoryMaster:
                    if (_taskId == _rootTaskId)
                    {
                        checkpoint = state.Checkpoint();
                        checkpoint.OperatorId = OperatorId;
                        checkpoint.SubscriptionName = SubscriptionName;
                        _checkpointService.Checkpoint(checkpoint);
                    }
                    break;
                case CheckpointLevel.PersistentMemoryAll:
                    checkpoint = state.Checkpoint();
                    OperatorId = OperatorId;
                    SubscriptionName = SubscriptionName;
                    _checkpointService.Checkpoint(checkpoint);
                    break;
                default:
                    throw new IllegalStateException("Checkpoint level not supported");
            }
        }

        public bool GetCheckpoint(out ICheckpointState checkpoint, int iteration = -1)
        {
            if (InternalCheckpoint != null && (iteration == -1 || InternalCheckpoint.Iteration == iteration))
            {
                checkpoint = InternalCheckpoint;
                return true;
            }

            return _checkpointService.GetCheckpoint(out checkpoint, _taskId, SubscriptionName, OperatorId, iteration, false);
        }

        public void WaitCompletionBeforeDisposing(CancellationTokenSource cancellationSource)
        {
            if (_taskId == _rootTaskId)
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
            _commLayer.TopologyUpdateRequest(_taskId, OperatorId);
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
            if (message.MessageType != DriverMessageType.Topology)
            {
                throw new IllegalStateException("Message not appropriate for Broadcast Topology");
            }

            var rmsg = message as TopologyMessagePayload;

            if (rmsg.ToRemove == true)
            {
                lock (_toRemove)
                {
                    foreach (var list in rmsg.TopologyUpdates)
                    {
                        foreach (var node in list)
                        {
                            var id = Utils.GetTaskNum(node);
                            _toRemove.TryAdd(id, new byte());
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
                        foreach (var id in _toRemove.Keys)
                        {
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

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            int retry = 0;

            if (_sendQueue.TryPeek(out message))
            {
                var dm = message as DataMessage;
                while (!_topologyUpdateReceived.WaitOne(_timeout))
                {
                    if (cancellationSource.IsCancellationRequested)
                    {
                        Logger.Log(Level.Warning, "Received cancellation request: stop sending");
                        return;
                    }

                    retry++;

                    if (retry > _retry)
                    {
                        throw new Exception(string.Format(
                            "Iteration {0}: Failed to send message to the next node in the ring after {1} try", dm.Iteration, _retry));
                    }

                    TopologyUpdateRequest();
                }

                _sendQueue.TryDequeue(out message);

                if (_taskId == _rootTaskId)
                {
                    _topologyUpdateReceived.Reset();
                }

                foreach (var node in _children.Where(x => !_toRemove.TryGetValue(x.Key, out byte val)))
                {
                    _commLayer.Send(node.Value, message, cancellationSource);
                }
            }
        }

        private void UpdateTopology(ref List<List<string>> updates)
        {
            List<string> toRemove = null;
            foreach (var list in updates)
            {
                if (list[0] == _taskId)
                {
                    toRemove = list;
                    for (int i = 1; i < list.Count; i++)
                    {
                        var id = Utils.GetTaskNum(list[i]);
                        if (!_toRemove.TryRemove(id, out byte value))
                        {
                            _children.TryAdd(id, list[i]);
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
