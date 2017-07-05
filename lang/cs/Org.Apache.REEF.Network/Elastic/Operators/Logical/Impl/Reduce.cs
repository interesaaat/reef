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
using Org.Apache.REEF.Network.Elastic.Driver.Policy;
using Org.Apache.REEF.Network.Elastic.Topology.Impl;
using Org.Apache.REEF.Network.Elastic.Topology;
using System;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    class Reduce : ElasticOperator
    {
        private const string _operator = "reduce";
        private int _receiverId;
    
        public Reduce(int receiverId, ElasticOperator prev, TopologyTypes topologyType, PolicyLevel policyLevel) : base(null)
        {
            _receiverId = receiverId;
            _prev = prev;
            _topology = topologyType == TopologyTypes.Flat ? (ITopology)new EmptyTopology() : (ITopology)new TreeTopology();
            _policy = policyLevel;
            _id = GetSubscription.GetNextOperatorId();
        }

        public int GetReceiverTaskId
        {
            get
            {
                return _receiverId;
            }
        }

        private int GenerateReceiverTaskId()
        {
            _receiverId = 1;
            return _receiverId;
        }

        protected new int GenerateMasterTaskId()
        {
            return GenerateReceiverTaskId();
        }

        protected new bool IsMasterTaskId(int taskId)
        {
            return _receiverId == taskId;
        }

        protected new int GetMasterTaskId
        {
            get
            {
                return _receiverId;
            }
        }

        public override void OnStopAndRecompute()
        {
            throw new NotImplementedException();
        }

        public override void OnStopAndResubmit()
        {
            throw new NotImplementedException();
        }
    }
}
