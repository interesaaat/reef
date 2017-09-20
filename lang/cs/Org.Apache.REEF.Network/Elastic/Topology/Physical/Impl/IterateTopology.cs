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
using System.Collections.Generic;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class IterateTopology : DriverAwareOperatorTopology, IDisposable, ICheckpointingTopology
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IterateTopology));

        private readonly CommunicationLayer _commLayer;

        [Inject]
        private IterateTopology(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscription,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(OperatorId))] int operatorId,
            CommunicationLayer commLayer,
            CheckpointService checkpointService) : base(taskId, rootId, subscription, operatorId)
        {
            _commLayer = commLayer;

            _commLayer.RegisterOperatorTopologyForDriver(_taskId, this);

            Service = checkpointService;
        }

        public CheckpointService Service { get; private set; }

        public ICheckpointState InternalCheckpoint { get; private set; }

        public void Checkpoint(ICheckpointState state)
        {
            switch (state.Level)
            {
                case CheckpointLevel.None:
                    break;
                case CheckpointLevel.EphemeralMaster:
                    InternalCheckpoint = state;
                    break;
                case CheckpointLevel.PersistentMemoryMaster:
                    if (_taskId == _rootTaskId)
                    {
                        state.OperatorId = OperatorId;
                        state.SubscriptionName = SubscriptionName;
                        Service.Checkpoint(state);
                    }
                    break;
                case CheckpointLevel.PersistentMemoryAll:
                    state.OperatorId = OperatorId;
                    state.SubscriptionName = SubscriptionName;
                    Service.Checkpoint(state);
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
            return Service.GetCheckpoint(iteration) as CheckpointState<List<GroupCommunicationMessage>>;
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

        public void Dispose()
        {
            Service.RemoveCheckpoint(OperatorId);
        }

        internal override void OnMessageFromDriver(IDriverMessagePayload message)
        {
        }

        internal override void OnFailureResponseMessageFromDriver(IDriverMessagePayload message)
        {
            Logger.Log(Level.Info, "Received failure recovery, going to resume computation from my checkpoint");

            var destMessage = message as FailureMessagePayload;
            InternalCheckpoint = GetCheckpoint();
        }
    }
}
