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
using Org.Apache.REEF.Network.Elastic.Operators.Logical;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal abstract class NToOneTopology<T> : OperatorTopologyWithCommunication, ICheckpointingTopology
    {
        private readonly ReduceFunction<T> _reducer;
        private readonly CheckpointService _checkpointService;
        private readonly ConcurrentDictionary<int, byte> _outOfOrderTopologyRemove;
        private readonly object _lock;

        private readonly ConcurrentQueue<GroupCommunicationMessage> _aggregationQueueData;
        private readonly HashSet<string> _aggregationQueueSources;
        private readonly ConcurrentDictionary<string, byte> _toRemove;

        private readonly bool _requestUpdate;
        private readonly ManualResetEvent _topologyUpdateReceived;

        protected NToOneTopology(
            string subscription,
            int rootId,
            ISet<int> children,
            string taskId,
            int operatorId,
            bool requestUpdate,
            int retry,
            int timeout,
            int disposeTimeout,
            ReduceFunction<T> reduceFunction,
            CommunicationLayer commLayer,
            CheckpointService checkpointService) : base(taskId, rootId, subscription, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _reducer = reduceFunction;
            _requestUpdate = requestUpdate;

            _checkpointService = checkpointService;
            _outOfOrderTopologyRemove = new ConcurrentDictionary<int, byte>();
            _topologyUpdateReceived = new ManualResetEvent(false);
            _lock = new object();

            _toRemove = new ConcurrentDictionary<string, byte>();
            _aggregationQueueSources = new HashSet<string>();

            if (_rootTaskId == _taskId)
            {
                _aggregationQueueSources.Add(_taskId);
            }

            _aggregationQueueData = new ConcurrentQueue<GroupCommunicationMessage>();

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

            Console.WriteLine("Received from {0}", message.SourceId.ToString());

            Merge(message.Data, message.SourceId.ToString());
        }

        internal override void OnFailureResponseMessageFromDriver(DriverMessagePayload value)
        {
            throw new NotImplementedException();
        }

        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                _commLayer.WaitForTaskRegistration(new List<string>() { _rootTaskId }, cancellationSource);
            }
            catch (Exception e)
            {
                throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + _taskId, e);
            }

            _initialized = true;

            Send(cancellationSource);
        }

        public override void WaitCompletionBeforeDisposing()
        {
            var elapsedTime = 0;
            while ((_sendQueue.Count > 0 || _aggregationQueueSources.Count() > 0) && elapsedTime < _disposeTimeout)
            {
                // The topology is still trying to send messages, wait
                Thread.Sleep(100);
                elapsedTime += 100;
            }
        }

        internal override void OnMessageFromDriver(DriverMessagePayload message)
        {
            if (message.MessageType != DriverMessageType.Topology)
            {
                throw new IllegalStateException("Message not appropriate for Reduce Topology");
            }

            var rmsg = message as TopologyMessagePayload;
            Console.WriteLine("Received message " + rmsg.ToRemove);

            foreach (var updates in rmsg.TopologyUpdates)
            {   
                if (rmsg.ToRemove)
                {
                    lock (_lock)
                    {
                        foreach (var node in updates.Children)
                        {
                            Console.WriteLine("Removing " + node);
                            _toRemove.TryAdd(node, 0);
                            _commLayer.RemoveConnection(node);
                        }
                    }
                }
                else
                {
                    if (updates.Node == _taskId)
                    {
                        lock (_lock)
                        {
                            foreach (var node in updates.Children)
                            {
                                var id = Utils.GetTaskNum(node);

                                if (!_children.TryAdd(id, node))
                                {
                                    if (!_toRemove.TryRemove(node, out byte val))
                                    {
                                        Logger.Log(Level.Warning, string.Format("Trying to remove unknown task {0}: ignoring", node));
                                    }
                                }
                            }

                            if (_rootTaskId != _taskId)
                            {
                                _rootTaskId = updates.Root;
                                Console.WriteLine("Updated Root: " + _rootTaskId);
                            }
                            Console.WriteLine("Updated Children: " + string.Join(",", _children.Values));
                        }
                    }
                }
            }

            Console.WriteLine("Root: " + _rootTaskId);
            Console.WriteLine("Children: " + string.Join(",", _children.Values.Where(x => !_toRemove.ContainsKey(x))));

            if (!rmsg.ToRemove)
            {
                _topologyUpdateReceived.Set();
                Console.WriteLine("Setting");
            }

            Reduce();
        }

        internal override void Send(GroupCommunicationMessage message, CancellationTokenSource cancellationSource)
        {
            if (_requestUpdate)
            {
                Console.WriteLine("Waiting");
                TopologyUpdateRequest();
                _topologyUpdateReceived.WaitOne();
                Console.WriteLine("After Waiting");
            }

            if (_children.Where(x => !_toRemove.TryGetValue(x.Value, out byte val)).Count() == 0 && _initialized)
            {
                Console.WriteLine("sending directly");
                _topologyUpdateReceived.Reset();
                _sendQueue.Enqueue(message);
                Send(new CancellationTokenSource());
            }
            else
            {
                Merge(message, _taskId);
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            while (_sendQueue.TryDequeue(out message) && !cancellationSource.IsCancellationRequested)
            {
                Console.WriteLine("reduce sending to {0}", _rootTaskId);
                _commLayer.Send(_rootTaskId, message, cancellationSource);
            }
        }

        private void Merge(GroupCommunicationMessage message, string taskId)
        {
            lock (_lock)
            {
                var msg = message as DataMessage<T>;

                if (_aggregationQueueSources.Add(taskId))
                {
                    if (_aggregationQueueData.Count() <= 1 || !_reducer.CanMerge)
                    {
                        _aggregationQueueData.Enqueue(message);
                    }
                    else
                    {
                        _reducer.Reduce(_aggregationQueueData, message as DataMessage<T>);
                    }
                }
                else
                {
                    Console.WriteLine("Message for node {0} already in queue.", taskId);
                }

                if (_topologyUpdateReceived.WaitOne(1))
                {
                    Reduce();
                }
            }
        }

        private void Reduce()
        { 
            lock (_lock)
            {
                if (_topologyUpdateReceived.WaitOne(1) && _aggregationQueueSources.Where(x => !_toRemove.TryGetValue(x, out byte val)).Count() > _children.Where(x => !_toRemove.TryGetValue(x.Value, out byte val)).Count())
                {
                    GroupCommunicationMessage finalAggregatedMessage;
                    if (!_reducer.CanMerge)
                    {
                        ////int diff = _taskId == _rootTaskId ? 1 : 0;
                        ////if (_aggregationQueueData.Count != _aggregationQueueSources.Count() - diff)
                        ////{
                        ////    throw new IllegalStateException(string.Format("Number of partially aggregated messages {0} do not match with count {1}", _aggregationQueueData.Count, _aggregationQueueSources.Count() - diff));
                        ////}
                        _reducer.Reduce(_aggregationQueueData);
                    }
                    
                    if (!_aggregationQueueData.TryDequeue(out finalAggregatedMessage))
                    {
                        throw new IllegalStateException("Message not found");
                    }

                    if (_aggregationQueueData.Count > 0)
                    {
                        throw new IllegalStateException("Pipeline of aggregation message not supported");
                    }

                    Console.WriteLine("sending because got all {0} messages", _children.Where(x => !_toRemove.TryGetValue(x.Value, out byte val)).Count());
                    _aggregationQueueSources.Clear();
                    _topologyUpdateReceived.Reset();
                    Console.WriteLine("Resetting");

                    foreach (var node in _toRemove.Keys)
                    {
                        var id = Utils.GetTaskNum(node);
                        _children.TryRemove(id, out string task);
                    }
                    _toRemove.Clear();

                    if (_rootTaskId == _taskId)
                    {
                        _aggregationQueueSources.Add(_taskId);
                        _messageQueue.Add(finalAggregatedMessage);
                    }
                    else if (_initialized)
                    {
                        _sendQueue.Enqueue(finalAggregatedMessage);
                        Send(new CancellationTokenSource());
                    }
                }
                else
                {
                    Console.WriteLine("Missing nodes {0}", string.Join(",", _children.Where(x => !_toRemove.TryGetValue(x.Value, out byte val)).Where(x => !_aggregationQueueSources.Where(y => _toRemove.TryGetValue(y, out byte val)).Contains(x.Value))));
                }
            }
        }
    }
}
