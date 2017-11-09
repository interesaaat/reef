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
using Org.Apache.REEF.Network.Elastic.Comm;
using System;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public sealed class DefaultTaskSetSubscription : 
        IElasticTaskSetSubscription,
        IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskSetSubscription));

        private bool _finalized;
        private volatile bool _scheduled;
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
            _scheduled = false;
            _numTasks = numTasks;
            _tasksAdded = 0;
            _missingMasterTasks = new HashSet<string>();
            Completed = false;
            Service = elasticService;
            _defaultFailureMachine = failureMachine ?? new DefaultFailureStateMachine(numTasks);
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

        public bool Completed { get; set; }

        public int GetNextOperatorId()
        {
            return Interlocked.Increment(ref _numOperators);
        }

        public bool AddTask(string taskId)
        {
            if (Completed || (_scheduled && FailureStatus.FailureState == (int)DefaultFailureStates.Fail))
            {
                return false;
            }

            if (!_finalized)
            {
                throw new IllegalStateException(
                    "CommunicationGroupDriver must call Build() before adding tasks to the group.");
            }

            lock (_tasksLock)
            {
                // We don't add a task if eventually we end up by not adding the master task
                if (!_scheduled && (_tasksAdded >= _numTasks || 
                    (_tasksAdded + _missingMasterTasks.Count >= _numTasks && !_missingMasterTasks.Contains(taskId))))
                {
                    return false;
                }

                if (!RootOperator.AddTask(taskId))
                {
                    return true;
                }

                _tasksAdded++;

                _missingMasterTasks.Remove(taskId);

                _defaultFailureMachine.AddDataPoints(1, true);
            }

            return true;
        }

        public bool ScheduleSubscription()
        {
            if (_defaultFailureMachine.State.FailureState < (int)DefaultFailureStates.StopAndReschedule && RootOperator.CanBeScheduled())
            {
                _scheduled = true;

                RootOperator.BuildState();
            }

            return _scheduled;
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

        public void OnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            RootOperator.OnTaskMessage(message, ref returnMessages);
        }

        public void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents)
        {
            // Failures have to be propagated down to the operators
            RootOperator.OnTaskFailure(task, ref failureEvents);
        }

        public void EventDispatcher(ref IFailureEvent @event)
        {
            switch ((DefaultFailureStateEvents)@event.FailureEvent)
            {
                case DefaultFailureStateEvents.Reconfigure:
                    var rec = @event as IReconfigure;
                    OnReconfigure(ref rec);
                    break;
                case DefaultFailureStateEvents.Reschedule:
                    var res = @event as IReschedule;
                    OnReschedule(ref res);
                    break;
                case DefaultFailureStateEvents.Stop:
                    var stp = @event as IStop;
                    OnStop(ref stp);
                    break;
                default:
                    OnFail();
                    break;
            }

            RootOperator.EventDispatcher(ref @event);
        }

        public void OnReconfigure(ref IReconfigure info)
        {
            lock (_statusLock)
            {
                FailureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReconfigure));
            }
        }

        public void OnReschedule(ref IReschedule rescheduleEvent)
        {
            lock (_statusLock)
            {
                FailureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReschedule));
            }
        }

        public void OnStop(ref IStop stopEvent)
        {
            lock (_statusLock)
            {
                FailureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.StopAndReschedule));
            }
        }

        public void OnFail()
        {
            lock (_statusLock)
            {
                FailureStatus = FailureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.Fail));
            }
        }
    }
}