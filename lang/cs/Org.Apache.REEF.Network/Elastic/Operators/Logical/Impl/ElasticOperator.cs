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
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;

/// <summary>
/// Basic implementation for logical operators.
/// Each operator is part of a subscription and requires a topology, a failure
/// state machine and a checkpoint policy.
/// </summary>
namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    public abstract class ElasticOperator : IFailureResponse, ITaskMessageResponse
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

        protected IElasticTaskSetSubscription _subscription;
        protected int _id;

        protected IConfiguration[] _configurations;

        public ElasticOperator(
            IElasticTaskSetSubscription subscription,
            ElasticOperator prev,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel level = CheckpointLevel.None,
            params IConfiguration[] configurations)
        {
            _subscription = subscription;
            _prev = prev;
            _id = Subscription.GetNextOperatorId();
            _topology = topology;
            _failureMachine = failureMachine;
            _checkpointLevel = level;
            _configurations = configurations;
        }

        public int MasterId { get; protected set; }

        public string OperatorName { get; protected set; }

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

        internal virtual void GatherMasterIds(ref HashSet<string> missingMasterTasks)
        {
            if (_operatorFinalized != true)
            {
                throw new IllegalStateException("Operator need to be build before finalizing the subscription");
            }

            missingMasterTasks.Add(Utils.BuildTaskId(Subscription.SubscriptionName, MasterId));

            if (_next != null)
            {
                _next.GatherMasterIds(ref missingMasterTasks);
            }
        }

        public void ChangePolicy(IFailureStateMachine level)
        {
            throw new NotImplementedException();
        }

        public abstract ElasticOperator Broadcast<T>(int senderTaskId, ITopology topology = null, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        public ElasticOperator Broadcast<T>(TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Broadcast<T>(MasterId, new FullConnectedTopology(MasterId), failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
        }

        public ElasticOperator Broadcast<T>(TopologyTypes topologyType, params IConfiguration[] configurations)
        {
            return Broadcast<T>(MasterId, topologyType == TopologyTypes.Flat ? (ITopology)new FlatTopology(MasterId) : (ITopology)new TreeTopology(MasterId), _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public ElasticOperator Broadcast<T>(int senderTaskId, params IConfiguration[] configurations)
        {
            return Broadcast<T>(senderTaskId, new FlatTopology(senderTaskId), _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public abstract ElasticOperator AggregationRing<T>(int coordinatorTaskId, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        public ElasticOperator AggregationRing<T>(params IConfiguration[] configurations)
        {
            return AggregationRing<T>(MasterId, _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public abstract ElasticOperator Reduce(int receiverTaskId, TopologyTypes topologyType, IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel, params IConfiguration[] configurations);

        public ElasticOperator Reduce(TopologyTypes topologyType = TopologyTypes.Flat, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return Reduce(MasterId, topologyType, failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
        }

        public ElasticOperator Reduce(int receiverTaskId, params IConfiguration[] configurations)
        {
            return Reduce(receiverTaskId, TopologyTypes.Flat, _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public abstract ElasticOperator ConditionalIterate(int coordinatorId, ITopology topology = null, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        public abstract ElasticOperator EnumerableIterate(int masterTaskId, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        public ElasticOperator Iterate(TopologyTypes topologyType, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            return ConditionalIterate(MasterId, topologyType == TopologyTypes.Flat ? (ITopology)new FlatTopology(MasterId) : (ITopology)new TreeTopology(MasterId), failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
        }

        public ElasticOperator Iterate(int coordinatorTaskId, params IConfiguration[] configurations)
        {
            return ConditionalIterate(coordinatorTaskId, new FlatTopology(coordinatorTaskId), _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public ElasticOperator Iterate(IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel, params IConfiguration[] configurations)
        {
            return EnumerableIterate(MasterId, failureMachine, checkpointLevel, configurations);
        }

        public ElasticOperator Iterate(params IConfiguration[] configurations)
        {
            return EnumerableIterate(MasterId, _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public ElasticOperator Scatter(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat)
        {
            throw new NotImplementedException();
        }

        public ISet<RingReturnMessage> OnTaskMessage(ITaskMessage message)
        {
            var replies = ReactOnTaskMessage(message);

            if (_next != null)
            {
                replies.UnionWith(_next.OnTaskMessage(message));
            }

            return replies;
        }

        public IFailureState OnTaskFailure(IFailedTask task)
        {
            var exception = task.AsError() as OperatorException;

            if (Subscription.IsIterative || exception.OperatorId <= _id)
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

            foreach (var conf in _configurations)
            {
                operatorConf = Configurations.Merge(operatorConf, conf);
            }

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
               "State for Operator {0} in Subscription {1}:\n", OperatorName, Subscription.SubscriptionName);
            string topologyState = string.Format(CultureInfo.InvariantCulture, "Topology:\n{0}", _topology.LogTopologyState());
            string failureMachineState = "Failure State: " + _failureMachine.State.FailureState +
                    "\nFailure(s) Reported: " + _failureMachine.NumOfFailedDataPoints;

            LOGGER.Log(Level.Info, intro + topologyState + failureMachineState);
        }

        protected virtual ISet<RingReturnMessage> ReactOnTaskMessage(ITaskMessage message)
        {
            return new HashSet<RingReturnMessage>();
        }
    }
}
