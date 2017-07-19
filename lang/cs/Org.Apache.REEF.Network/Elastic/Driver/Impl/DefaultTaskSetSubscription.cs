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
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl;
using System.Threading;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public sealed class DefaultTaskSetSubscription : 
        IElasticTaskSetSubscription,
        IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskSetSubscription));

        private readonly string _subscriptionName;
        private bool _finalized;
        private IFailureState _failureState;
        private readonly AvroConfigurationSerializer _confSerializer;
        private readonly IElasticTaskSetService _elasticService;

        private readonly int _numTasks;
        private int _tasksAdded;

        private IFailureStateMachine _defaultFailureMachine;
        private ElasticOperator _root;
        private int _numOperators;

        // This is used for fault-tolerancy. Failures over an iterative pipeline of operators
        // have to be propagated through all operators.
        private int _iteratorId;

        private readonly object _tasksLock;
        private readonly object _statusLock;

        internal DefaultTaskSetSubscription(
            string subscriptionName,
            AvroConfigurationSerializer confSerializer,
            int numTasks,
            IElasticTaskSetService elasticService,
            IFailureStateMachine failureMachine = null)
        {
            _confSerializer = confSerializer;
            _subscriptionName = subscriptionName;
            _finalized = false;
            _numTasks = numTasks;
            _tasksAdded = 0;
            _elasticService = elasticService;
            _defaultFailureMachine = failureMachine ?? new DefaultFailureStateMachine();
            _failureState = new DefaultFailureState();
            _root = new DefaultEmpty(this, _defaultFailureMachine.Clone());

            _iteratorId = -1;

            _tasksLock = new object();
            _statusLock = new object();
        }

        public string SubscriptionName
        {
            get
            {
                return _subscriptionName;
            }
        }

        public int GetNextOperatorId()
        {
            return Interlocked.Increment(ref _numOperators);
        }

        public bool AddTask(string taskId)
        {
            if (!_finalized)
            {
                throw new IllegalStateException(
                    "CommunicationGroupDriver must call Build() before adding tasks to the group.");
            }

            lock (_tasksLock)
            {
                if (_tasksAdded >= _numTasks)
                {
                    return false;
                }

                RootOperator.AddTask(taskId);

                _tasksAdded++;

                if(_tasksAdded == _numTasks)
                {
                    RootOperator.BuildState();
                }
            }

            return true;
        }

        // For the moment this method will always return true.
        // In the future we  may implement different policies for 
        // triggering the scheduling of the tasks.
        public bool ScheduleSubscription
        {
            get
            {
                return true;
            }
        }

        public bool IsMasterTaskContext(IActiveContext activeContext)
        {
            if (!_finalized)
            {
                throw new IllegalStateException(
                    "Driver must call Build() before checking IsMasterTaskContext.");
            }

            int id = Utils.GetContextNum(activeContext);
            return id == 1;
        }

        public IElasticTaskSetService Service
        {
            get
            {
                return _elasticService;
            }
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder builder, int taskId)
        {
            builder = builder
                .BindSetEntry<GroupCommConfigurationOptions.CommunicationGroupNames, string>(
                    GenericType<GroupCommConfigurationOptions.CommunicationGroupNames>.Class,
                    _subscriptionName);

                RootOperator.GetElasticTaskConfiguration(ref builder, taskId);
        }

        public ElasticOperator RootOperator
        {
            get
            {
                return _root;
            }
        }

        public IElasticTaskSetSubscription Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Subscription cannot be built more than once");
            }

            _finalized = true;

            return this;
        }

        public int IteratorId
        {
            get
            {
                return _iteratorId;
            }
            set
            {
                _iteratorId = value;
            }
        }

        public IFailureState FailureState
        {
            get
            {
                return _failureState;
            }
        }

        public IFailureState OnTaskFailure(IFailedTask task)
        {
            // Failure have to be propagated down to the operators
            var status = RootOperator.OnTaskFailure(task);

            // Update the current subscription status
            lock (_statusLock)
            {
                if (status.FailureState < _failureState.FailureState)
                {
                    throw new IllegalStateException("A failure cannot improve the failure status of the subscription");
                }

                _failureState.FailureState = status.FailureState;
            }

            // Failure have to be propagated up to the service
            Service.OnTaskFailure(task);

            return _failureState;
        }

        public void EventDispatcher(IFailureEvent @event)
        {
            RootOperator.EventDispatcher(@event);

            switch ((DefaultFailureStateEvents)@event.FailureEvent)
            {
                case DefaultFailureStateEvents.Reconfigure:
                    OnReconfigure(@event as IReconfigure);
                    break;
                case DefaultFailureStateEvents.Reschedule:
                    OnReschedule(@event as IReschedule);
                    break;
                case DefaultFailureStateEvents.Stop:
                    OnStop(@event as IStop);
                    break;
            }

            Service.EventDispatcher(@event);
        }

        public void OnReconfigure(IReconfigure info)
        {
            LOGGER.Log(Level.Info, "Reconfiguring subscription " + _subscriptionName);
        }

        public void OnReschedule(IReschedule rescheduleEvent)
        {
            LOGGER.Log(Level.Info, "Going to reschedule a task for subscription " + _subscriptionName);
        }

        public void OnStop(IStop stopEvent)
        {
            LOGGER.Log(Level.Info, "Going to stop subscription" + _subscriptionName + " and reschedule a task");
        }
    }
}