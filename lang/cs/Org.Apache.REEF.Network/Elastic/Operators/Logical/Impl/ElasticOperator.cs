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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Impl;
using Org.Apache.REEF.Utilities.Logging;
using System.Globalization;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Tang.Util;

/// <summary>
/// Basic implementation for logical operators.
/// Each operator is part of a subscription and requires a topology, a failure
/// state machine and a checkpoint policy.
/// </summary>
namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    public abstract class ElasticOperator : IFailureResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticOperator));

        // For the moment we consider only linear sequences of operators (no branching for e.g., joins)
        protected ElasticOperator _next = null;
        protected ElasticOperator _prev = null;

        protected IFailureStateMachine _failureMachine;
        protected CheckpointLevel _checkpointLevel;
        protected ITopology _topology;

        protected bool _stateFinalized = false;
        protected bool _operatorFinalized = false;

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
            _id = Subscription.GetNextOperatorId();
            _topology = topology;
            _failureMachine = failureMachine;
            _checkpointLevel = level;
        }

        public bool AddTask(string taskId)
        {
            _topology.AddTask(taskId);

            // The assumption is that only one data point is added for each task
            _failureMachine.AddDataPoints(1);

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

        protected virtual string OperatorName
        {
            get { return string.Empty; }
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

        protected IElasticTaskSetSubscription Subscription
        {
            get
            {
                if (_subscription == null)
                {
                    if (_prev == null)
                    {
                        throw new IllegalStateException("The reference to the parent subscription is lost");
                    }

                    return _prev.Subscription;
                }

                return _subscription;
            }
        }

        public void GetElasticTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            if (_operatorFinalized && _stateFinalized)
            {
                GetOperatorConfiguration(ref confBuilder, taskId);

                if (_next != null)
                {
                    _next.GetElasticTaskConfiguration(ref confBuilder, taskId);
                }
            }
            else
            {
                throw new IllegalStateException("Operator need to be Build before getting tasks configuration");
            }
        }

        public ElasticOperator Build()
        {
            if (_operatorFinalized == true)
            {
                throw new IllegalStateException("Operator cannot be built more than once");
            }

            if (_prev != null)
            {
                _prev.Build();
            }

            _operatorFinalized = true;

            return this;
        }

        public ElasticOperator BuildState()
        {
            if (_stateFinalized == true)
            {
                throw new IllegalStateException("Operator cannot be built more than once");
            }

            if (_operatorFinalized != true)
            {
                throw new IllegalStateException("Operator need to be build before finalizing its state");
            }

            if (_next != null)
            {
                _next.BuildState();
            }

            _failureMachine.Build();
            _topology.Build();

            LogOperatorState();

            _stateFinalized = true;
           
            return this;
        }

        public void ChangePolicy(IFailureStateMachine level)
        {
            throw new NotImplementedException();
        }

        public abstract ElasticOperator Broadcast<T>(int senderTaskId, ITopology topology = null, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        public ElasticOperator Broadcast<T>(TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            var senderId = GenerateMasterTaskId();
            return Broadcast<T>(senderId, topologyType == TopologyTypes.Flat ? (ITopology)new FlatTopology(senderId) : (ITopology)new TreeTopology(senderId), failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
        }

        public ElasticOperator Broadcast<T>(TopologyTypes topologyType, params IConfiguration[] configurations)
        {
            var senderId = GenerateMasterTaskId();
            return Broadcast<T>(GenerateMasterTaskId(), topologyType == TopologyTypes.Flat ? (ITopology)new FlatTopology(senderId) : (ITopology)new TreeTopology(senderId), _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public ElasticOperator Broadcast<T>(int senderTaskId, params IConfiguration[] configurations)
        {
            return Broadcast<T>(senderTaskId, new FlatTopology(senderTaskId), _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public abstract ElasticOperator Reduce(int receiverTaskId, TopologyTypes topologyType, IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel, params IConfiguration[] configurations);

        public ElasticOperator Reduce(TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Reduce(GenerateMasterTaskId(), topologyType, failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
        }

        public ElasticOperator Reduce(int receiverTaskId, params IConfiguration[] configurations)
        {
            return Reduce(receiverTaskId, TopologyTypes.Flat, _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public abstract ElasticOperator Iterate(int masterTaskId, TopologyTypes topologyType, IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel, params IConfiguration[] configurations);

        public ElasticOperator Iterate(TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Iterate(GenerateMasterTaskId(), topologyType, failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
        }

        public ElasticOperator Iterate(int masterTaskId, params IConfiguration[] configurations)
        {
            return Iterate(masterTaskId, TopologyTypes.Flat, _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public ElasticOperator Scatter(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat)
        {
            throw new NotImplementedException();
        }

        public IFailureState OnTaskFailure(IFailedTask task)
        {
            var exception = task.AsError() as OperatorException;

            if (Subscription.IteratorId > 0 || exception.OperatorId <= _id)
            {
                int lostDataPoints = _topology.RemoveTask(task.Id);
                IFailureState result = _failureMachine.RemoveDataPoints(lostDataPoints);

                LogOperatorState();

                if (_next != null)
                {
                    result = result.Merge(_next.OnTaskFailure(task));
                }

                return result;
            }
            else
            {
                if (_next != null)
                {
                    return _next.OnTaskFailure(task);
                }
                else
                {
                    return _failureMachine.State;
                }
            }
        }

        public abstract void EventDispatcher(IFailureEvent @event);

        protected virtual void GetOperatorConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            ICsConfigurationBuilder operatorBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            _topology.GetTaskConfiguration(ref operatorBuilder, taskId);

            PhysicalOperatorConfiguration(ref operatorBuilder);

            IConfiguration operatorConf = operatorBuilder
                .BindNamedParameter<OperatorsConfiguration.OperatorId, int>(
                    GenericType<OperatorsConfiguration.OperatorId>.Class,
                    _id.ToString(CultureInfo.InvariantCulture))
                .Build();

            Subscription.Service.SerializeOperatorConfiguration(ref confBuilder, operatorConf);
        }

        protected abstract void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder);

        protected static void SetMessageType(Type operatorType, ref ICsConfigurationBuilder confBuilder)
        {
            if (operatorType.IsGenericType)
            {
                var genericTypes = operatorType.GenericTypeArguments;
                var msgType = genericTypes[0];
                confBuilder.BindNamedParameter<OperatorsConfiguration.MessageType, string>(
                    GenericType<OperatorsConfiguration.MessageType>.Class, msgType.AssemblyQualifiedName);
            }
        }

        protected virtual void LogOperatorState()
        {
            string intro = string.Format(CultureInfo.InvariantCulture,
               "State for Operator {0} in Subscription {1}:\n", Subscription.SubscriptionName, OperatorName);
            string topologyState = string.Format(CultureInfo.InvariantCulture, "Topology:\n{0}", _topology.LogTopologyState());
            string failureMachineState = "Failure State: " + _failureMachine.State.FailureState +
                    "\nFailure(s) Reported: " + _failureMachine.NumOfFailedDataPoints;

            LOGGER.Log(Level.Info, intro + topologyState + failureMachineState);
        }
    }
}
