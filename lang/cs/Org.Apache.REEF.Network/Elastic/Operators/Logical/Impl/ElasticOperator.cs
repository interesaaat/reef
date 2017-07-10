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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    public abstract class ElasticOperator : IFailureResponse
    {
        // For the moment we consider only linear sequences of operators (no branching for e.g., joins)
        protected ElasticOperator _next = null;
        protected ElasticOperator _prev = null;

        protected IFailureStateMachine _failureMachine;
        protected CheckpointLevel _checkpointLevel;
        protected ITopology _topology;

        protected bool _finalized = false;

        protected int _masterTaskId = 0;
        protected IElasticTaskSetSubscription _subscription;
        protected int _id;

        public ElasticOperator(
            IElasticTaskSetSubscription subscription,
            ElasticOperator prev,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel level = CheckpointLevel.None)
        {
            _subscription = subscription;
            _prev = prev;
            _id = GetSubscription.GetNextOperatorId();
            _topology = topology;
            _failureMachine = failureMachine;
            _checkpointLevel = level;
        }

        public bool AddTask(string taskId)
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

        public void EnsurePolicy(FailureState level)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Broadcast(int senderTaskId, TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            _next = new Broadcast(senderTaskId, this, topologyType, failureMachine ?? _failureMachine.Clone, checkpointLevel, configurations);
            return _next;
        }

        public ElasticOperator Broadcast(TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Broadcast(GenerateMasterTaskId(), topologyType, failureMachine ?? _failureMachine.Clone, checkpointLevel, configurations);
        }

        public ElasticOperator Broadcast(TopologyTypes topologyType, params IConfiguration[] configurations)
        {
            return Broadcast(GenerateMasterTaskId(), topologyType, _failureMachine.Clone, CheckpointLevel.None, configurations);
        }

        public ElasticOperator Broadcast(int senderTaskId, params IConfiguration[] configurations)
        {
            return Broadcast(senderTaskId, TopologyTypes.Flat, new DefaultFailureStateMachine(), CheckpointLevel.None, configurations);
        }

        public ElasticOperator Reduce(int receiverTaskId, TopologyTypes topologyType, IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel, params IConfiguration[] configurations)
        {
            _next = new Reduce(receiverTaskId, this, topologyType, failureMachine, checkpointLevel, configurations);
            return _next;
        }

        public ElasticOperator Reduce(TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Reduce(GenerateMasterTaskId(), topologyType, failureMachine ?? _failureMachine.Clone, checkpointLevel, configurations);
        }

        public ElasticOperator Reduce(int receiverTaskId, params IConfiguration[] configurations)
        {
            return Reduce(receiverTaskId, TopologyTypes.Flat, _failureMachine.Clone, CheckpointLevel.None, configurations);
        }

        public ElasticOperator Iterate(int masterTaskId, TopologyTypes topologyType, IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel, params IConfiguration[] configurations)
        {
            _next = new Iterate(masterTaskId, this, topologyType, failureMachine, checkpointLevel, configurations);
            return _next;
        }

        public ElasticOperator Iterate(TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Iterate(GenerateMasterTaskId(), topologyType, failureMachine ?? _failureMachine.Clone, checkpointLevel, configurations);
        }

        public ElasticOperator Iterate(int masterTaskId, params IConfiguration[] configurations)
        {
            return Iterate(masterTaskId, TopologyTypes.Flat, _failureMachine.Clone, CheckpointLevel.None, configurations);
        }

        public ElasticOperator Scatter(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat)
        {
            throw new NotImplementedException();
        }

        public FailureStateEvent OnTaskFailure(IFailedTask task)
        {
            return FailureStateEvent.Continue;
        }

        public void OnContinueAndReconfigure()
        {
            throw new NotImplementedException();
        }

        public void OnContinueAndReschedule()
        {
            throw new NotImplementedException();
        }

        public void OnStopAndReschedule()
        {
            throw new NotImplementedException();
        }
    }
}
