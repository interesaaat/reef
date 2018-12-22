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
using Org.Apache.REEF.Network.Elastic.Failures.Enum;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal abstract class NToOneTopology<T> : OperatorTopologyWithCommunication, ICheckpointingTopology
    {
        protected static readonly Logger LOGGER = Logger.GetLogger(typeof(NToOneTopology<>));

        private readonly ReduceFunction<T> _reducer;
        private readonly CentralizedCheckpointService _checkpointService;
        private readonly ConcurrentDictionary<int, byte> _outOfOrderTopologyRemove;
        private readonly object _lock;

        private readonly Queue<Tuple<string, GroupCommunicationMessage>> _aggregationQueueData;
        private readonly HashSet<string> _aggregationQueueSources;
        private readonly ConcurrentDictionary<string, byte> _toRemove;

        private readonly bool _requestUpdate;
        private readonly ManualResetEvent _topologyUpdateReceived;

        protected NToOneTopology(
            string subscription,
            string rootTaskId,
            ISet<int> children,
            string taskId,
            int operatorId,
            bool requestUpdate,
            int retry,
            int timeout,
            int disposeTimeout,
            ReduceFunction<T> reduceFunction,
            CommunicationService commLayer,
            CentralizedCheckpointService checkpointService) : base(taskId, rootTaskId, subscription, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _reducer = reduceFunction;
            _requestUpdate = requestUpdate;

            _checkpointService = checkpointService;
            _outOfOrderTopologyRemove = new ConcurrentDictionary<int, byte>();
            _topologyUpdateReceived = new ManualResetEvent(false);
            _lock = new object();

            _toRemove = new ConcurrentDictionary<string, byte>();
            _aggregationQueueSources = new HashSet<string>();

            if (RootTaskId == TaskId)
            {
                _aggregationQueueSources.Add(TaskId);
            }

            _aggregationQueueData = new Queue<Tuple<string, GroupCommunicationMessage>>();

            _commService.RegisterOperatorTopologyForTask(this);
            _commService.RegisterOperatorTopologyForDriver(this);

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

        public void Checkpoint(ICheckpointableState state, int iteration)
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

            return _checkpointService.GetCheckpoint(out checkpoint, TaskId, SubscriptionName, OperatorId, iteration, false);
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

        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                _commService.WaitForTaskRegistration(new List<string>() { RootTaskId }, cancellationSource);
            }
            catch (Exception e)
            {
                throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + TaskId, e);
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

        /// <summary>
        /// Handler for messages coming from the driver.
        /// </summary>
        /// <param name="message">Message from the driver</param>
        public override void OnNext(DriverMessagePayload message)
        {
            switch (message.PayloadType)
            {
                case DriverMessagePayloadType.Failure:
                    {
                        var rmsg = message as TopologyMessagePayload;

                        lock (_lock)
                        {
                            foreach (var updates in rmsg.TopologyUpdates)
                            {
                                foreach (var node in updates.Children)
                                {
                                    _toRemove.TryAdd(node, 0);
                                    _commService.RemoveConnection(node);
                                }
                            }
                        }
                    }
                    break;
                case DriverMessagePayloadType.Update:
                    {
                        var rmsg = message as TopologyMessagePayload;

                        foreach (var updates in rmsg.TopologyUpdates)
                        {
                            if (updates.Node == TaskId)
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
                                                LOGGER.Log(Level.Warning, string.Format("Trying to remove unknown task {0}: ignoring", node));
                                            }
                                        }
                                    }

                                    if (RootTaskId != TaskId)
                                    {
                                        RootTaskId = updates.Root;
                                        Console.WriteLine("Updated Root: " + RootTaskId);
                                    }
                                    Console.WriteLine("Updated Children: " + string.Join(",", _children.Values));
                                }
                            }

                            _topologyUpdateReceived.Set();
                        }
                    }
                    break;
                default:
                    throw new ArgumentException($"Message type {message.PayloadType} not supported by N to One topologies.");
            }

            Reduce();
        }

        public override void Send(GroupCommunicationMessage message, CancellationTokenSource cancellationSource)
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
                Merge(message, TaskId);
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            while (_sendQueue.TryDequeue(out message) && !cancellationSource.IsCancellationRequested)
            {
                Console.WriteLine("reduce sending to {0}", RootTaskId);
                _commService.Send(RootTaskId, message, cancellationSource);
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
                        _aggregationQueueData.Enqueue(Tuple.Create(taskId, message));
                    }
                    else
                    {
                        _reducer.OnlineReduce(_aggregationQueueData, message as DataMessage<T>);
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
                    if (_reducer.CanMerge)
                    {
                        finalAggregatedMessage = _aggregationQueueData.Dequeue().Item2;
                    }
                    else
                    {
                        finalAggregatedMessage = _reducer.Reduce(_aggregationQueueData);
                    }
                    
                    Console.WriteLine("sending because got all {0} messages", _children.Where(x => !_toRemove.TryGetValue(x.Value, out byte val)).Count());
                    _aggregationQueueSources.Clear();
                    _topologyUpdateReceived.Reset();
                    _aggregationQueueData.Clear();
                    Console.WriteLine("Resetting");

                    foreach (var node in _toRemove.Keys)
                    {
                        var id = Utils.GetTaskNum(node);
                        _children.TryRemove(id, out string task);
                    }
                    _toRemove.Clear();

                    if (RootTaskId == TaskId)
                    {
                        _aggregationQueueSources.Add(TaskId);
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
