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

using Org.Apache.REEF.Driver.Task;
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
using Org.Apache.REEF.Network.Elastic.Config;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public sealed class DefaultTaskSetSubscription : 
        IElasticTaskSetSubscription,
        IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskSetSubscription));

        private bool _finalized;
        private readonly AvroConfigurationSerializer _confSerializer;

        private readonly int _numTasks;
        private int _tasksAdded;
        private HashSet<string> _missingMasterTasks;

        private IFailureStateMachine _defaultFailureMachine;
        private int _numOperators;

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
            SubscriptionName = subscriptionName;
            _finalized = false;
            _numTasks = numTasks;
            _tasksAdded = 0;
            _missingMasterTasks = new HashSet<string>();
            Service = elasticService;
            _defaultFailureMachine = failureMachine ?? new DefaultFailureStateMachine();
            FailureStatus = new DefaultFailureState();
            RootOperator = new DefaultEmpty(this, _defaultFailureMachine.Clone());

            IsIterative = false;

            _tasksLock = new object();
            _statusLock = new object();
        }

        public string SubscriptionName { get; set; }

        public ElasticOperator RootOperator { get; private set; }

        public IElasticTaskSetService Service { get; private set; }

        public bool IsIterative { get; set; }

        public IFailureState FailureStatus { get; private set; }

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
                // We don't add a task if eventually we end up by not adding the master task
                if (_tasksAdded >= _numTasks || 
                    (_tasksAdded + _missingMasterTasks.Count >= _numTasks && !_missingMasterTasks.Contains(taskId)))
                {
                    return false;
                }

                RootOperator.AddTask(taskId);

                _tasksAdded++;

                _missingMasterTasks.Remove(taskId);

                if (_tasksAdded == _numTasks)
                {
                    RootOperator.BuildState();
                }
            }

            return true;
        }

        public bool ScheduleSubscription()
        {
            return true;
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

        public void GetTaskConfiguration(ref ICsConfigurationBuilder builder, int taskId)
        {
            builder = builder
                .BindNamedParameter<GroupCommunicationConfigurationOptions.SubscriptionName, string>(
                    GenericType<GroupCommunicationConfigurationOptions.SubscriptionName>.Class,
                    SubscriptionName);

                RootOperator.GetTaskConfiguration(ref builder, taskId);
        }

        public IElasticTaskSetSubscription Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Subscription cannot be built more than once");
            }

            RootOperator.GatherMasterIds(ref _missingMasterTasks);

            _finalized = true;

            return this;
        }

        public void OnTaskMessage(ITaskMessage message, ref List<DriverMessage> returnMessages)
        {
            RootOperator.OnTaskMessage(message, ref returnMessages);
        }

        public void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents)
        {
            // Failure have to be propagated down to the operators
            RootOperator.OnTaskFailure(task, ref failureEvents);
        }

        public void EventDispatcher(IFailureEvent @event, ref List<DriverMessage> failureResponses)
        {
            RootOperator.EventDispatcher(@event, ref failureResponses);

            switch ((DefaultFailureStateEvents)@event.FailureEvent)
            {
                case DefaultFailureStateEvents.Reconfigure:
                    failureResponses.AddRange(OnReconfigure(@event as IReconfigure));
                    break;
                case DefaultFailureStateEvents.Reschedule:
                    failureResponses.AddRange(OnReschedule(@event as IReschedule));
                    break;
                case DefaultFailureStateEvents.Stop:
                    failureResponses.AddRange(OnStop(@event as IStop));
                    break;
            }
        }

        public List<DriverMessage> OnReconfigure(IReconfigure info)
        {
            LOGGER.Log(Level.Info, "Reconfiguring subscription " + SubscriptionName);

            lock (_statusLock)
            {
                FailureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReconfigure));
            }

            return new List<DriverMessage>();
        }

        public List<DriverMessage> OnReschedule(IReschedule rescheduleEvent)
        {
            LOGGER.Log(Level.Info, "Going to reschedule a task for subscription " + SubscriptionName);

            lock (_statusLock)
            {
                FailureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReschedule));
            }

            return new List<DriverMessage>();
        }

        public List<DriverMessage> OnStop(IStop stopEvent)
        {
            LOGGER.Log(Level.Info, "Going to stop subscription" + SubscriptionName + " and reschedule a task");

            lock (_statusLock)
            {
                FailureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.StopAndReschedule));
            }

            return new List<DriverMessage>();
        }
    }
}