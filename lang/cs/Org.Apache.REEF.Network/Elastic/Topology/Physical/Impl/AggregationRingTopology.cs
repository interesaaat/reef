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
        private BlockingCollection<string> _next;

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
            _next = new BlockingCollection<string>();

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

        internal override GroupCommunicationMessage Receive(CancellationTokenSource cancellationSource)
        {
            return Receive(cancellationSource, -1);
        }

        internal GroupCommunicationMessage Receive(CancellationTokenSource cancellationSource, int iteration)
        {
            GroupCommunicationMessage message;

            _messageQueue.TryTake(out message, Timeout.Infinite, cancellationSource.Token);

            return message;
        }

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
            if (_taskId != _rootTaskId)
            {
                while (_commLayer.Lookup(_rootTaskId) == true && !cancellationSource.IsCancellationRequested)
                {
                    Thread.Sleep(100);
                }
            }
        }

        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            if (_rootTaskId != _taskId)
            {
                try
                {
                    _commLayer.WaitForTaskRegistration(new List<string> { _rootTaskId }, cancellationSource);
                }
                catch (Exception e)
                {
                    throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + _taskId, e);
                }
            }

            _initialized = true;
        }

        public override void OnNext(NsMessage<GroupCommunicationMessage> message)
        {
            if (!_initialized)
            {
                Logger.Log(Level.Warning, "Received data while task is not initialized: ignoring");
                return;
            }

            if (_messageQueue.IsAddingCompleted)
            {
                if (_messageQueue.Count > 0)
                {
                    throw new IllegalStateException("Trying to add messages to a closed non-empty queue");
                }
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

        internal override void OnMessageFromDriver(DriverMessagePayload message)
        {
            if (message.MessageType != DriverMessageType.Ring)
            {
                throw new IllegalStateException("Message not appropriate for Aggregation Ring Topology");
            }

            var rmsg = message as RingMessagePayload;

            if (_sendQueue.Count > 0)
            {
                _next.TryAdd(rmsg.NextTaskId);
            }
            else
            {
                Logger.Log(Level.Info, "Going to resume ring computation for " + rmsg.NextTaskId);

                if (!GetCheckpoint(out ICheckpointState checkpoint, rmsg.Iteration) || checkpoint.State.GetType() != typeof(GroupCommunicationMessage[]))
                {
                    Logger.Log(Level.Warning, "Failure recovery from state not available: propagating the request");
                    _commLayer.NextDataRequest(_taskId, rmsg.Iteration);
                    return;
                }

                var cancellationSource = new CancellationTokenSource();

                foreach (var data in checkpoint.State as GroupCommunicationMessage[])
                {
                    _commLayer.Send(rmsg.NextTaskId, data, cancellationSource);
                }
            }
        }

        internal override void OnFailureResponseMessageFromDriver(DriverMessagePayload message)
        {
            switch (message.MessageType)
            {
                case DriverMessageType.Resume:
                    var msg = "Received resume message: going to resume ring computation for ";
                    var destMessage = message as ResumeMessagePayload;

                    Logger.Log(Level.Info, msg + destMessage.NextTaskId + " in iteration " + destMessage.Iteration);

                    ICheckpointState checkpoint;

                    if (!GetCheckpoint(out checkpoint, destMessage.Iteration) || checkpoint.State.GetType() != typeof(GroupCommunicationMessage[]))
                    {
                        var splits = Operator.FailureInfo.Split(':');

                        var iteration = destMessage.Iteration;
                        if (_rootTaskId == _taskId)
                        {
                            iteration--;
                        }
                        Logger.Log(Level.Warning, "I am blocked as well: propagating the request");
                        _commLayer.NextDataRequest(_taskId, iteration);
                        return;
                    }

                    var cancellationSource = new CancellationTokenSource();

                    foreach (var data in checkpoint.State as GroupCommunicationMessage[])
                    {
                        _commLayer.Send(destMessage.NextTaskId, data, cancellationSource);
                    }
                    break;
                default:
                    throw new IllegalStateException("Failure Message not supported");
            }
        }

        internal void JoinTheRing(int iteration)
        {
            if (_taskId != _rootTaskId)
            {
                // This is required because data (coming from when the task was alive)
                // could have been received while a task recovers from a failure. 
                while (_messageQueue.Count > 0)
                {
                    _messageQueue.Take();
                }
                _commLayer.JoinTheRing(_taskId);
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            int retry = 0;
            GroupCommunicationMessage message;
            string nextNode;

            if (_sendQueue.TryPeek(out message))
            {
                var dm = message as DataMessage;

                _commLayer.TokenRequest(_taskId, dm.Iteration);

                while (!_next.TryTake(out nextNode, _timeout))
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

                    _commLayer.TokenRequest(_taskId, dm.Iteration);
                }

                _sendQueue.TryDequeue(out message);

                _commLayer.Send(nextNode, message, cancellationSource);
            }
        }
    }
}
