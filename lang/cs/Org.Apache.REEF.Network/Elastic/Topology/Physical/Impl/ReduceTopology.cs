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
using Org.Apache.REEF.Network.Elastic.Operators.Logical;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class ReduceTopology<T> : OperatorTopologyWithCommunication, ICheckpointingTopology
    {
        private readonly ReduceFunction<T> _reducer;
        private readonly CheckpointService _checkpointService;
        private readonly ConcurrentDictionary<int, byte> _outOfOrderTopologyRemove;
        private readonly object _lock;

        private readonly ConcurrentQueue<GroupCommunicationMessage> _aggregationQueue;
        private volatile int _aggregationCount;

        private readonly ManualResetEvent _topologyUpdateReceived;

        [Inject]
        private ReduceTopology(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscription,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(OperatorId))] int operatorId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int disposeTimeout,
            ReduceFunction<T> reduceFunction,
            CommunicationLayer commLayer,
            CheckpointService checkpointService) : base(taskId, rootId, subscription, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _reducer = reduceFunction;

            _checkpointService = checkpointService;
            _outOfOrderTopologyRemove = new ConcurrentDictionary<int, byte>();
            _topologyUpdateReceived = new ManualResetEvent(_rootTaskId == taskId ? false : true);
            _lock = new object();

            _aggregationCount = 0;
            _aggregationQueue = new ConcurrentQueue<GroupCommunicationMessage>();

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

            foreach (var payload in message.Data)
            {
                _aggregationCount++;

                if (_aggregationCount == 1 || !_reducer.CanMerge)
                {
                    _aggregationQueue.Enqueue(payload);
                }
                else
                {
                    lock (_lock)
                    {
                        if (!_aggregationQueue.TryDequeue(out GroupCommunicationMessage partial))
                        {
                            throw new IllegalStateException("Element not found");
                        }

                        var nextPartial = _reducer.Reduce(new List<GroupCommunicationMessage>() { payload, partial });
                        _aggregationQueue.Enqueue(nextPartial);
                    }
                }
            }

            if (_aggregationCount >= _children.Count)
            {
                GroupCommunicationMessage finalAggregatedMessage;
                if (!_reducer.CanMerge)
                {
                    if (_aggregationQueue.Count != _aggregationCount)
                    {
                        throw new IllegalStateException("Number of partially aggregated messages do not match");
                    }
                    finalAggregatedMessage = _reducer.Reduce(_aggregationQueue);
                }
                else
                {
                    if (!_aggregationQueue.TryDequeue(out finalAggregatedMessage))
                    {
                        throw new IllegalStateException("Message not found");
                    }

                    if (_aggregationQueue.Count > 0)
                    {
                        throw new IllegalStateException("Pipeline of aggregation message not supported");
                    }
                }

                _aggregationCount = 0;

                if (_rootTaskId == _taskId)
                {
                    _messageQueue.Add(finalAggregatedMessage);
                }
                else if (_initialized)
                {
                    _sendQueue.Enqueue(finalAggregatedMessage);
                    Send(new CancellationTokenSource());
                }
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
                foreach (var list in rmsg.TopologyUpdates)
                {
                    foreach (var node in list)
                    {
                        var id = Utils.GetTaskNum(node);
                        if (!_children.TryRemove(id, out string value))
                        {
                            _outOfOrderTopologyRemove.TryAdd(id, new byte());
                        }
                    }
                }
            }
            else
            {
                Logger.Log(Level.Warning, "Received a topology update message from driver but sending queue is empty: ignoring");
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            while (_sendQueue.TryDequeue(out message) && !cancellationSource.IsCancellationRequested)
            {
                 _commLayer.Send(_rootTaskId, message, cancellationSource);
            }
        }
    }
}
