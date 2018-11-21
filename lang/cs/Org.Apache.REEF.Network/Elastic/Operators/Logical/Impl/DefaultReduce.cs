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
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using System.Globalization;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Reduce operator implementation.
    /// </summary>
    class DefaultReduce<T> : DefaultNToOne<T>, IElasticReduce
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultReduce<>));

        public DefaultReduce(
            int receiverId,
            ElasticOperator prev,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                receiverId,
                prev,
                topology,
                failureMachine,
                checkpointLevel,
                configurations)
        {
            OperatorName = Constants.Reduce;
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder.BindImplementation(GenericType<IElasticTypedOperator<T>>.Class, GenericType<Physical.Impl.DefaultReduce<T>>.Class)
                       .BindNamedParameter<RequestTopologyUpdate, bool>(
                            GenericType<RequestTopologyUpdate>.Class,
                            (_topology.GetType() == typeof(TreeTopology)).ToString(CultureInfo.InvariantCulture));
            SetMessageType(typeof(Physical.Impl.DefaultReduce<T>), ref confBuilder);
        }
    }
}
