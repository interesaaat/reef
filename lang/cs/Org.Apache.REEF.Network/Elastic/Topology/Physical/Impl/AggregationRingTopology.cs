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
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class AggregationRingTopology : OperatorTopologyWithCommunication, ICheckpointingTopology
    {
        private ConcurrentDictionary<int, string> _next;
        private readonly ManualResetEvent _sendmre;

        private readonly CheckpointService _checkpointService;

        [Inject]
        private AggregationRingTopology(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscription,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(OperatorId))] int operatorId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int disposeTimeout,
            CommunicationLayer commLayer,
            CheckpointService checkpointService) : base(taskId, rootId, subscription, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _next = new ConcurrentDictionary<int, string>();

            _sendmre = new ManualResetEvent(false);

            foreach (var child in children)
            {
                var childTaskId = Utils.BuildTaskId(SubscriptionName, child);

                _children.TryAdd(child, childTaskId);
            }

            _commLayer.RegisterOperatorTopologyForTask(_taskId, this);

            _commLayer.RegisterOperatorTopologyForDriver(_taskId, this);

            _checkpointService = checkpointService;
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

        public ICheckpointState GetCheckpoint(int iteration = -1)
        {
            if (InternalCheckpoint != null && (iteration == -1 || InternalCheckpoint.Iteration == iteration))
            {
                return InternalCheckpoint;
            }
            return _checkpointService.GetCheckpoint(_taskId, SubscriptionName, OperatorId, iteration);
        }

        public override void WaitCompletionBeforeDisposing()
        {
            if (_taskId != _rootTaskId)
            {
                while (_commLayer.Lookup(_rootTaskId) == true)
                {
                    Thread.Sleep(100);
                }
            }
        }

        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            if (_rootTaskId != _taskId)
            {
                _commLayer.WaitForTaskRegistration(new List<string> { _rootTaskId }, cancellationSource);
            }

            _initialized = true;
        }

        public override void OnNext(NsMessage<GroupCommunicationMessage> message)
        {
            if (_messageQueue.IsAddingCompleted)
            {
                if (_messageQueue.Count > 0)
                {
                    throw new IllegalStateException("Trying to add messages to a closed non-empty queue");
                }
                _messageQueue = new BlockingCollection<GroupCommunicationMessage>();
            }

            Console.WriteLine("Received from {0}", message.SourceId);

            foreach (var payload in message.Data)
            {
                _messageQueue.Add(payload);
            }
        }

        public new void Dispose()
        {
            base.Dispose();

            _checkpointService.RemoveCheckpoint(_taskId, SubscriptionName, OperatorId);
        }

        internal override void OnMessageFromDriver(IDriverMessagePayload message)
        {
            if (message.MessageType != DriverMessageType.Ring)
            {
                throw new IllegalStateException("Message not appropriate for Aggregation Ring Topology");
            }

            var data = message as RingMessagePayload;
            string value;
            if (_next.TryRemove(data.Iteration, out value))
            {
                Console.WriteLine("I was supposed to send to {0} in iteration {1}", data.Iteration, value);
            }
            _next.TryAdd(data.Iteration, data.NextTaskId);

            Console.WriteLine("Going to send message to {0} in iteration {1}", data.NextTaskId, data.Iteration);

            _sendmre.Set();
        }

        internal override void OnFailureResponseMessageFromDriver(IDriverMessagePayload message)
        {
            if (message.MessageType == DriverMessageType.Request)
            {
                var destMessage = message as TokenReceivedRequest;
                var str = (int)PositionTracker.InReceive + ":" + destMessage.Iteration;

                Console.WriteLine(str);
                Console.WriteLine(Operator.FailureInfo);

                if (Operator.FailureInfo != str)
                {
                    Logger.Log(Level.Info, "Received failure request for iteration {0}: I am ok", destMessage.Iteration);
                }
                else
                {
                    Logger.Log(Level.Info, "Received failure request for iteration {0}: I am blocked", destMessage.Iteration);
                }

                _commLayer.TokenResponse(_taskId, destMessage.Iteration, Operator.FailureInfo != str);
            }

            if (message.MessageType == DriverMessageType.Failure)
            {
                var msg = "Received failure recovery: ";
                var destMessage = message as FailureMessagePayload;

                GroupCommunicationMessage gcm;
                if (_sendQueue.TryPeek(out gcm))
                {
                    var dm = gcm as DataMessage;
                    if (dm.Iteration == destMessage.Iteration)
                    {
                        Logger.Log(Level.Info, msg + "going to send message to " + destMessage.NextTaskId);

                        _next.TryAdd(destMessage.Iteration, destMessage.NextTaskId);
                    }
                }
                else
                {
                    Logger.Log(Level.Info, msg + "going to resume ring computation from my checkpoint");

                    var checkpoint = GetCheckpoint();

                    if (checkpoint == null || checkpoint.State.GetType() != typeof(GroupCommunicationMessage[]))
                    {
                        throw new IllegalStateException("Failure recovery from state not available");
                    }

                    foreach (var data in checkpoint.State as GroupCommunicationMessage[])
                    {
                        _commLayer.Send(destMessage.NextTaskId, data);
                    }
                }
            }
        }

        internal void JoinTheRing(int iteration)
        {
            if (_taskId != _rootTaskId)
            {
                _commLayer.JoinTheRing(_taskId, iteration);
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            int retry = 0;
            GroupCommunicationMessage message;
            string nextNode;

            Console.WriteLine("Sendqueue size is " + _sendQueue.Count());
            if (_sendQueue.TryPeek(out message))
            {
                var dm = message as DataMessage;
                while (!_next.TryGetValue(dm.Iteration, out nextNode))
                {
                    if (cancellationSource.IsCancellationRequested)
                    {
                        Logger.Log(Level.Warning, "Received cancellation request: stop sending");
                        return;
                    }

                    _sendmre.Reset();
                    Console.WriteLine("Waiting inside loop ");
                    if (!_sendmre.WaitOne(_timeout))
                    {
                        retry++;
                        _commLayer.NextTokenRequest(_taskId, dm.Iteration);
                        if (retry > _retry)
                        {
                            throw new Exception(string.Format(
                                "Iteration {0}: Failed to send message to the next node in the ring after {1} try", dm.Iteration, _retry));
                        }
                    }
                }

                _sendmre.Reset();

                _sendQueue.TryDequeue(out message);

                Console.WriteLine("Sending to " + nextNode);

                _commLayer.Send(nextNode, message);
            }
        }
    }
}
