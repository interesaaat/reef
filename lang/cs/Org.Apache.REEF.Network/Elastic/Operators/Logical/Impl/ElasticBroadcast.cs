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
    class ElasticBroadcast : ElasticOperator
    {
        private const string _operator = "broadcast";
        private string _senderId;
    
        public ElasticBroadcast(string senderId, ElasticOperator prev, TopologyTypes topologyType, PolicyLevel policyLevel)
        {
            _prev = prev;
            _topology = topologyType == TopologyTypes.Flat ? (ITopology)new FlatTopology() : (ITopology)new TreeTopology();
            _policy = policyLevel;
        }

        public string GetSenderTaskId
        {
            get
            {
                return _senderId;
            }
        }

        protected new bool IsMasterTaskId
        {
            get
            {
                return _senderId == null;
            }
        }

        protected new void SetMasterTaskId(string taskId)
        {
            _senderId = taskId;
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
