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

using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Network.Elastic.Topology.Impl;
using Org.Apache.REEF.Network.Elastic.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Broadcast operator implementation.
    /// </summary>
    class DefaultBroadcast : ElasticOperatorWithDefaultDispatcher, IElasticBroadcast
    {
        private const string _operator = "broadcast";
        private int _senderId;
    
        public DefaultBroadcast(
            int senderId, 
            ElasticOperator prev, 
            TopologyTypes topologyType, 
            IFailureStateMachine failureMachine, 
            CheckpointLevel checkpointLevel, 
            params IConfiguration[] configurations) : base(
                null, 
                prev, 
                topologyType == TopologyTypes.Flat ? (ITopology)new FlatTopology(senderId) : (ITopology)new TreeTopology(senderId), 
                failureMachine,
                checkpointLevel)
        {
            _senderId = senderId;
        }

        public int GetSenderTaskId
        {
            get
            {
                return _senderId;
            }
        }

        protected int GenerateSenderTaskId()
        {
            _senderId = 1;
            return _senderId;
        }

        protected new int GenerateMasterTaskId()
        {
            return GenerateSenderTaskId();
        }

        protected new bool IsMasterTaskId(int taskId)
        {
            return _senderId == taskId;
        }
    }
}
