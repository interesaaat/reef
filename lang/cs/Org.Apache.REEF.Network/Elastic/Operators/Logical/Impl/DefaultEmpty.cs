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

using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Empty operator implementing the default failure logic. To use only as root.
    /// </summary>
    class DefaultEmpty : ElasticOperatorWithDefaultDispatcher
    {
        public DefaultEmpty(IElasticTaskSetSubscription subscription, IFailureStateMachine filureMachine) : 
            base(subscription, null, new EmptyTopology(), filureMachine)
        {
            OperatorName = Constants.Empty;
            MasterId = 1;
        }

        internal override void GatherMasterIds(ref HashSet<string> missingMasterTasks)
        {
            if (_operatorFinalized != true)
            {
                throw new IllegalStateException("Operator need to be build before finalizing the subscription");
            }

            if (_next != null)
            {
                _next.GatherMasterIds(ref missingMasterTasks);
            }
        }

        protected override void LogOperatorState()
        {
        }

        protected override void GetOperatorConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
        }

        public override void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents)
        {
            if (_next != null)
            {
                _next.OnTaskFailure(task, ref failureEvents);
            }
        }
    }
}
