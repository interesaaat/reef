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

using System;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Exceptions;
using System.Globalization;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    public abstract class ElasticOperator : FailureResponse
    {
        // For the moment we consider only linear sequences of operators (no branching for e.g., joins)
        protected ElasticOperator _next = null;
        protected ElasticOperator _prev = null;

        protected PolicyLevel _policy;
        protected CheckpointLevel _checkpointLevel;
        protected ITopology _topology;

        protected bool _finalized = false;

        protected int _masterTaskId = 0;
        protected IElasticTaskSetSubscription _subscription;
        protected int _id;

        public ElasticOperator(IElasticTaskSetSubscription subscription)
        {
            _subscription = subscription;
        }

        public bool AddTask(int taskId)
        {
            _topology.AddTask(taskId);

            if (_next != null)
            {
                _next.AddTask(taskId);
            }

            return true;
        }

        protected int GenerateMasterTaskId()
        {
            _masterTaskId = 1;
            return _masterTaskId;
        }

        public bool IsAnyMasterTaskId(int id)
        {
            if (IsMasterTaskId(id))
            {
                return true;
            }
            
            if (_next != null)
            {
                return _next.IsAnyMasterTaskId(id);
            }

            return false;
        }

        protected bool IsMasterTaskId(int id)
        {
            return _masterTaskId == id;
        }

        protected int GetMasterTaskId
        {
            get
            {
                if (_masterTaskId < 1)
                {
                    throw new IllegalStateException("Master Task Id not set");
                }

                return _masterTaskId;
            }
        }

        protected IElasticTaskSetSubscription GetSubscription
        {
            get
            {
                if (_subscription == null)
                {
                    if (_prev == null)
                    {
                        throw new IllegalStateException("The reference to the parent subscription is lost");
                    }

                    return _prev.GetSubscription;
                }

                return _subscription;
            }
        }

        public void GetElasticTaskConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            if (_next != null)
            {
                _next.GetElasticTaskConfiguration(ref confBuilder);
            }
        }

        public ElasticOperator Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Operator cannot be built more than once");
            }

            if (_prev != null)
            {
                _prev.Build();
            }

            _finalized = true;
            return this;
        }

        protected void Reset()
        {
            throw new NotImplementedException();
        }

        public void EnsurePolicy(PolicyLevel level)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Broadcast(int senderTaskId, TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.Ignore, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            _next = new Broadcast(senderTaskId, this, topologyType, policyLevel, checkpointLevel, configurations);
            return _next;
        }

        public ElasticOperator Broadcast(TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.Ignore, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Broadcast(GenerateMasterTaskId(), topologyType, policyLevel, checkpointLevel, configurations);
        }

        public ElasticOperator Reduce(int receiverTaskId, TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.Ignore, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            _next = new Reduce(receiverTaskId, this, topologyType, policyLevel, checkpointLevel, configurations);
            return _next;
        }

        public ElasticOperator Reduce(TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.Ignore, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Reduce(GenerateMasterTaskId(), topologyType, policyLevel, checkpointLevel, configurations);
        }

        public ElasticOperator Iterate(int masterTaskId, TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.Ignore, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            _next = new Iterate(masterTaskId, this, topologyType, policyLevel, checkpointLevel, configurations);
            return _next;
        }

        public ElasticOperator Iterate(TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.Ignore, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Iterate(GenerateMasterTaskId(), topologyType, policyLevel, checkpointLevel, configurations);
        }

        public ElasticOperator Scatter(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat)
        {
            throw new NotImplementedException();
        }

        public override void OnNext(IFailedEvaluator value)
        {
            _next.OnNext(value);
        }

        public override void OnNext(IFailedTask value)
        {
            _next.OnNext(value);
        }
    }
}
