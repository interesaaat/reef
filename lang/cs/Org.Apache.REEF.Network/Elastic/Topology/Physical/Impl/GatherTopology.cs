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
using Org.Apache.REEF.Network.Elastic.Operators;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class GatherTopology<T> : NToOneTopology<T[]>
    {
        [Inject]
        private GatherTopology(
            [Parameter(typeof(OperatorParameters.SubscriptionName))] string subscriptionName,
            [Parameter(typeof(OperatorParameters.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(OperatorParameters.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(OperatorParameters.OperatorId))] int operatorId,
            [Parameter(typeof(OperatorParameters.RequestTopologyUpdate))] bool requestUpdate,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int disposeTimeout,
            CommunicationService commLayer,
            CheckpointService checkpointService) : base(
                subscriptionName,
                Utils.BuildTaskId(subscriptionName, rootId), 
                children,
                taskId, 
                operatorId, 
                requestUpdate,
                retry,
                timeout,
                disposeTimeout,
                new UnionFunction<T>(),
                commLayer,
                checkpointService)
        {
        }
    }
}
