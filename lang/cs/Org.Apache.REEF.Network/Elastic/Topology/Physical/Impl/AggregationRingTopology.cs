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
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class AggregationRingTopology : OperatorTopologyWithCommunication, ICheckpointingTopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AggregationRingTopology));

        private BlockingCollection<string> _next;

        private readonly CheckpointService _checkpointService;

        [Inject]
        private AggregationRingTopology(
            [Parameter(typeof(OperatorParameters.SubscriptionName))] string subscriptionName,
            [Parameter(typeof(OperatorParameters.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(OperatorParameters.OperatorId))] int operatorId,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int disposeTimeout,
            CommunicationService commLayer,
            CheckpointService checkpointService) : base(taskId, Utils.BuildTaskId(subscriptionName, rootId), subscriptionName, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _next = new BlockingCollection<string>();
            _checkpointService = checkpointService;

            _commService.RegisterOperatorTopologyForTask(this);
            _commService.RegisterOperatorTopologyForDriver(this);
        }

        public ICheckpointState InternalCheckpoint { get; private set; }

        internal IElasticOperator Operator { get; set; }

        public override GroupCommunicationMessage Receive(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;

            _messageQueue.TryTake(out message, Timeout.Infinite, cancellationSource.Token);

            return message;
        }

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

        public void WaitCompletionBeforeDisposing(CancellationTokenSource cancellationSource)
        {
            if (TaskId != RootTaskId)
            {
                while (_commService.Lookup(RootTaskId) == true && !cancellationSource.IsCancellationRequested)
                {
                    Thread.Sleep(100);
                }
            }
        }

        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            if (RootTaskId != TaskId)
            {
                try
                {
                    var tmp = new ConcurrentDictionary<int, string>();
                    tmp.TryAdd(0, RootTaskId);
                    _commService.WaitForTaskRegistration(new List<string>() { RootTaskId }, cancellationSource);
                }
                catch (Exception e)
                {
                    throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + TaskId, e);
                }
            }

            _initialized = true;
        }

        public override void OnNext(NsMessage<GroupCommunicationMessage> message)
        {
            if (!_initialized)
            {
                LOGGER.Log(Level.Warning, "Received data while task is not initialized: ignoring");
                return;
            }

            if (_messageQueue.IsAddingCompleted)
            {
                throw new IllegalStateException("Trying to add messages to a closed non-empty queue");
            }

            _messageQueue.Add(message.Data);
        }

        public new void Dispose()
        {
            base.Dispose();

            _checkpointService.RemoveCheckpoint(SubscriptionName, OperatorId);
        }

        /// <summary>
        /// Handler for messages coming from the driver.
        /// </summary>
        /// <param name="message">Message from the driver</param>
        public override void OnNext(DriverMessagePayload message)
        {
            switch (message.PayloadType)
            {
                case DriverMessagePayloadType.Ring:
                    {
                        var rmsg = message as RingMessagePayload;

                        if (_sendQueue.Count > 0)
                        {
                            _next.TryAdd(rmsg.NextTaskId);
                        }
                        else
                        {
                            LOGGER.Log(Level.Info, "Going to resume ring computation for " + rmsg.NextTaskId);

                            if (!GetCheckpoint(out ICheckpointState checkpoint, rmsg.Iteration) || checkpoint.State.GetType() != typeof(GroupCommunicationMessage[]))
                            {
                                LOGGER.Log(Level.Warning, "Failure recovery from state not available: propagating the request");
                                _commService.NextDataRequest(TaskId, rmsg.Iteration);
                                return;
                            }

                            var cancellationSource = new CancellationTokenSource();

                            foreach (var data in checkpoint.State as GroupCommunicationMessage[])
                            {
                                _commService.Send(rmsg.NextTaskId, data, cancellationSource);
                            }
                        }
                    }
                    break;
                case DriverMessagePayloadType.Resume:
                    {
                        var msg = "Received resume message: going to resume ring computation for ";
                        var destMessage = message as ResumeMessagePayload;

                        LOGGER.Log(Level.Info, msg + destMessage.NextTaskId + " in iteration " + destMessage.Iteration);

                        ICheckpointState checkpoint;

                        if (!GetCheckpoint(out checkpoint, destMessage.Iteration) || checkpoint.State.GetType() != typeof(GroupCommunicationMessage[]))
                        {
                            var splits = Operator.FailureInfo.Split(':');

                            var iteration = destMessage.Iteration;
                            if (RootTaskId == TaskId)
                            {
                                iteration--;
                            }
                            LOGGER.Log(Level.Warning, "I am blocked as well: propagating the request");
                            _commService.NextDataRequest(TaskId, iteration);
                            return;
                        }

                        var cancellationSource = new CancellationTokenSource();

                        foreach (var data in checkpoint.State as GroupCommunicationMessage[])
                        {
                            _commService.Send(destMessage.NextTaskId, data, cancellationSource);
                        }
                    }
                    break;
                default:
                    throw new ArgumentException($"Message type {message.PayloadType} not supported by aggregation ring topology.");
            }
        }

        public override void JoinTopology()
        {
            if (TaskId != RootTaskId)
            {
                // This is required because data (coming from when the task was alive)
                // could have been received while a task recovers from a failure. 
                while (_messageQueue.Count > 0)
                {
                    _messageQueue.Take();
                }
                _commService.JoinTopology(TaskId, OperatorId);
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

                _commService.TopologyUpdateRequest(TaskId, OperatorId);

                while (!_next.TryTake(out nextNode, _timeout))
                {
                    if (cancellationSource.IsCancellationRequested)
                    {
                        LOGGER.Log(Level.Warning, "Received cancellation request: stop sending");
                        return;
                    }

                    retry++;

                    if (retry > _retry)
                    {
                        throw new Exception(string.Format(
                            "Iteration {0}: Failed to send message to the next node in the ring after {1} try", dm.Iteration, _retry));
                    }

                    _commService.TopologyUpdateRequest(TaskId, OperatorId);
                }

                _sendQueue.TryDequeue(out message);

                _commService.Send(nextNode, message, cancellationSource);
            }
        }
    }
}
