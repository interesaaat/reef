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
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.NetworkService;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Task;
using System.Collections.Concurrent;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class IterateTopology : DriverAwareOperatorTopology, IDisposable, ICheckpointingTopology, IWaitForTaskRegistration
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IterateTopology));

        private readonly CommunicationLayer _commLayer;

        [Inject]
        private IterateTopology(
            [Parameter(typeof(OperatorParameters.SubscriptionName))] string subscriptionName,
            [Parameter(typeof(OperatorParameters.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(OperatorParameters.OperatorId))] int operatorId,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            CommunicationLayer commLayer,
            CheckpointService checkpointService) : base(taskId, Utils.BuildTaskId(subscriptionName, rootId), subscriptionName, operatorId)
        {
            _commLayer = commLayer;

            _commLayer.RegisterOperatorTopologyForDriver(TaskId, this);

            Service = checkpointService;

            Service.RegisterOperatorRoot(subscriptionName, operatorId, RootTaskId, RootTaskId == taskId);
        }

        public CheckpointService Service { get; private set; }

        public ICheckpointState InternalCheckpoint { get; private set; }

        public bool IsRoot
        {
            get { return RootTaskId == TaskId; }
        }

        public void IterationNumber(int iteration)
        {
            if (TaskId == RootTaskId)
            {
                _commLayer.IterationNumber(TaskId, OperatorId, iteration);
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
                    if (TaskId == RootTaskId)
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
                    if (TaskId == RootTaskId)
                    {
                        checkpoint = state.Checkpoint();
                        checkpoint.Iteration = iteration ?? -1;
                        checkpoint.OperatorId = OperatorId;
                        checkpoint.SubscriptionName = SubscriptionName;
                        Service.Checkpoint(checkpoint);
                    }
                    break;
                case CheckpointLevel.PersistentMemoryAll:
                    checkpoint = state.Checkpoint();
                    checkpoint.Iteration = iteration ?? -1;
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

            return Service.GetCheckpoint(out checkpoint, TaskId, SubscriptionName, OperatorId, iteration);
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            if (RootTaskId != TaskId)
            {
                try
                {
                    var tmp = new ConcurrentDictionary<int, string>();
                    tmp.TryAdd(0, RootTaskId);
                    _commLayer.WaitForTaskRegistration(new List<string>() { RootTaskId }, cancellationSource);
                }
                catch (Exception e)
                {
                    throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + TaskId, e);
                }
            }
        }

        public override void WaitCompletionBeforeDisposing()
        {
            if (TaskId != RootTaskId)
            {
                while (_commLayer.Lookup(RootTaskId) == true)
                {
                    Thread.Sleep(100);
                }
            }
        }

        public void Dispose()
        {
            Service.RemoveCheckpoint(TaskId, SubscriptionName, OperatorId);
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
