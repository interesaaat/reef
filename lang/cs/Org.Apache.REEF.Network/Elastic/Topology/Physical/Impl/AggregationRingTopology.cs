﻿// Licensed to the Apache Software Foundation (ASF) under one
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
        private static readonly Logger Logger = Logger.GetLogger(typeof(OperatorTopologyWithCommunication));

        private BlockingCollection<string> _next;

        private readonly CheckpointService _checkpointService;

        private readonly int _retry;
        private readonly int _timeout;

        [Inject]
        private AggregationRingTopology(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscription,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(OperatorId))] int operatorId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int disposeTimeout,
            CommunicationLayer commLayer,
            CheckpointService checkpointService) : base(taskId, rootId, subscription, operatorId, commLayer, disposeTimeout)
        {
            _next = new BlockingCollection<string>(1);

            _retry = retry;
            _timeout = timeout;

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
            _next.TryAdd(data.NextTaskId);
        }

        internal override void OnFailureResponseMessageFromDriver(IDriverMessagePayload message)
        {
            if (message.MessageType == DriverMessageType.Request)
            {
                Logger.Log(Level.Info, "Received failure request");

                var destMessage = message as TokenReceivedRequest;
                var str = (int)PositionTracker.InReceive + ":" + destMessage.Iteration;

                _commLayer.TokenResponse(_taskId, Operator.FailureInfo != str);
            }

            if (message.MessageType == DriverMessageType.Failure)
            {
                Logger.Log(Level.Info, "Received failure recovery, going to resume ring computation from my checkpoint");

                var destMessage = message as FailureMessagePayload;
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

        internal void JoinTheRing()
        {
            if (_taskId != _rootTaskId)
            {
                _commLayer.JoinTheRing(_taskId);
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            int retry = 0;
            GroupCommunicationMessage message;
            string nextNode;

            if (_sendQueue.TryDequeue(out message))
            {
                while (!_next.TryTake(out nextNode, _timeout, cancellationSource.Token))
                {
                    retry++;
                    var dm = message as DataMessage;
                    _commLayer.NextTokenRequest(_taskId, dm.Iteration);
                    if (retry > _retry)
                    {
                        throw new Exception("Failed to send message to the next node in the ring");
                    }
                }

                _commLayer.Send(nextNode, message);
            }
        }
    }
}
