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

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class BroadcastTopology : OperatorTopologyWithCommunication, ICheckpointingTopology
    {
        private readonly CheckpointService _checkpointService;

        private readonly ManualResetEvent _topologyUpdateReceived;

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
            CheckpointService checkpointService) : base(taskId, rootId, subscription, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _checkpointService = checkpointService;
            _topologyUpdateReceived = new ManualResetEvent(false);

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
                foreach (var node in rmsg.TopologyUpdates)
                {
                    var id = Utils.GetTaskNum(node);
                    _children.TryRemove(id, out string value);
                }
            }
            else if (_sendQueue.Count > 0)
            {
                foreach (var node in rmsg.TopologyUpdates)
                {
                    var id = Utils.GetTaskNum(node);
                    _children.TryAdd(id, node);
                }

                _topologyUpdateReceived.Set();
            }
            else
            {
                throw new MissingMethodException("TODO");
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

                    Console.WriteLine("Reseind");
                    TopologyUpdateRequest();
                }

                _sendQueue.TryDequeue(out message);
                _topologyUpdateReceived.Reset();

                foreach (var node in _children.Values)
                {
                    _commLayer.Send(node, message, cancellationSource);
                }
            }
        }
    }
}
