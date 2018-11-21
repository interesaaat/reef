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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using System;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Iterate operator implementation.
    /// </summary>
    class DefaultConditionalIterator : ElasticOperatorWithDefaultDispatcher, IElasticIterator
    {
        public DefaultConditionalIterator(
            int coordinatortId,
            ElasticOperator prev,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null,
                prev,
                null,
                failureMachine,
                checkpointLevel)
        {
            MasterId = coordinatortId;
            OperatorName = Constants.Iterate;
            WithinIteration = true;
        }

        public object Iteration
        {
            get { throw new NotImplementedException(); }
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
        }
    }
}
