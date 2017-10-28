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
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.NetworkService;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Task;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class IterateTopology : DriverAwareOperatorTopology, IDisposable, ICheckpointingTopology, IWaitForTaskRegistration
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

            Service.RegisterOperatorRoot(subscription, operatorId, _rootTaskId, _rootTaskId == taskId);
        }

        public CheckpointService Service { get; private set; }

        public ICheckpointState InternalCheckpoint { get; private set; }

        public bool IsRoot
        {
            get { return _rootTaskId == _taskId; }
        }

        public void IterationNumber(int iteration)
        {
            if (_taskId == _rootTaskId)
            {
                _commLayer.IterationNumber(_taskId, iteration);
            }
        }

        public void Checkpoint(ICheckpointableState state, int? iteration)
        {
            ICheckpointState checkpoint;

            switch (state.Level)
            {
                case CheckpointLevel.None:
                    break;
                case CheckpointLevel.EphemeralMaster:
                    if (_taskId == _rootTaskId)
                    {
                        checkpoint = state.Checkpoint();
                        checkpoint.Iteration = iteration ?? -1;
                        InternalCheckpoint = checkpoint;
                    }
                    break;
                case CheckpointLevel.EphemeralAll:
                    checkpoint = state.Checkpoint();
                    checkpoint.Iteration = iteration ?? -1;
                    InternalCheckpoint = checkpoint;
                    break;
                case CheckpointLevel.PersistentMemoryMaster:
                    if (_taskId == _rootTaskId)
                    {
                        checkpoint = state.Checkpoint();
                        checkpoint.Iteration = iteration ?? -1;
                        checkpoint.TaskId = _taskId;
                        checkpoint.OperatorId = OperatorId;
                        checkpoint.SubscriptionName = SubscriptionName;
                        Service.Checkpoint(checkpoint);
                    }
                    break;
                case CheckpointLevel.PersistentMemoryAll:
                    checkpoint = state.Checkpoint();
                    checkpoint.Iteration = iteration ?? -1;
                    checkpoint.TaskId = _taskId;
                    checkpoint.OperatorId = OperatorId;
                    checkpoint.SubscriptionName = SubscriptionName;
                    Service.Checkpoint(checkpoint);
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

            return Service.GetCheckpoint(out checkpoint, _taskId, SubscriptionName, OperatorId, iteration);
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            if (_rootTaskId != _taskId)
            {
                _commLayer.WaitForTaskRegistration(new List<string> { _rootTaskId }, cancellationSource);
            }
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
            Service.RemoveCheckpoint(_taskId, SubscriptionName, OperatorId);
        }

        internal override void OnMessageFromDriver(DriverMessagePayload message)
        {
        }

        internal override void OnFailureResponseMessageFromDriver(DriverMessagePayload message)
        {
            Logger.Log(Level.Info, "Received failure recovery, going to resume computation from my checkpoint");

            ICheckpointState checkpoint;
            if (GetCheckpoint(out checkpoint))
            {
                InternalCheckpoint = checkpoint;
            }
        }
    }
}
