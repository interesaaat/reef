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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Tang.Util;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Iterate operator implementation.
    /// </summary>
    class DefaultEnumerableIterator : ElasticOperatorWithDefaultDispatcher, IElasticIterator
    {
        public DefaultEnumerableIterator(
            int masterTaskId,
            ElasticOperator prev,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null, 
                prev, 
                new FlatTopology(masterTaskId), 
                failureMachine,
                checkpointLevel,
                configurations)
        {
            MasterId = masterTaskId;
            OperatorName = Constants.Iterate;
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

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder
                .BindImplementation(GenericType<IElasticTypedOperator<int>>.Class, GenericType<Physical.Impl.DefaultEnumerableIterator>.Class);
            SetMessageType(typeof(IElasticTypedOperator<int>), ref confBuilder);
        }
    }
}
