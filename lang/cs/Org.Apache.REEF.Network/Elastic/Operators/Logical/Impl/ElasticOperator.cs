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

using System;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Driver.Policy;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Exceptions;
using System.Globalization;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    public abstract class ElasticOperator : FailureResponse
    {
        // For the moment we consider only linear sequences of operators (no branching for e.g., joins)
        protected ElasticOperator _next;
        protected ElasticOperator _prev;

        protected PolicyLevel _policy;
        protected ITopology _topology;

        protected bool _finalized;

        protected string _masterTaskId;
        protected IElasticTaskSetSubscription _subscription;

        public void AddTask(string taskId)
        {
            _topology.AddTask(taskId);

            if (_next != null)
            {
                _next.AddTask(taskId);
            }
        }

        protected string GetMasterTaskId
        {
            get
            {
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

        protected string BuildMasterTaskId(string subscriptionName, string op)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", subscriptionName, op, new Random().Next());
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

        private ElasticOperator Broadcast<T>(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType, PolicyLevel policyLevel, params IConfiguration[] configurations)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Broadcast(string senderTaskId, TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.IGNORE, params IConfiguration[] configurations)
        {
            _next = new ElasticBroadcast(senderTaskId, this, topologyType, policyLevel);
            return _next;
        }

        public ElasticOperator Broadcast(TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.IGNORE, params IConfiguration[] configurations)
        {
            return Broadcast(_masterTaskId, topologyType, policyLevel, configurations);
        }

        public void GetElasticTaskConfiguration(out ICsConfigurationBuilder confBuilder)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Iterate<T>(Delegate condition, ElasticOperator prev, params IConfiguration[] configurations)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Reduce<T>(string receiverTaskId, ElasticOperator prev, TopologyTypes topologyType, params IConfiguration[] configurations)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Scatter(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat)
        {
            throw new NotImplementedException();
        }

        public new void OnNext(IFailedEvaluator value)
        {
            _next.OnNext(value);
        }

        public new void OnNext(IFailedTask value)
        {
            _next.OnNext(value);
        }
    }
}
