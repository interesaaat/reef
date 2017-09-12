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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public sealed class DefaultTaskSetManager : 
        ITaskSetManager, 
        IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskSetManager));

        private bool _finalized;

        private int _contextsAdded;
        private int _tasksAdded;
        private int _tasksRunning;
        private int _tasksFailed;
        private readonly int _numTasks;
        private float _failRatio;

        // Task info 0-indexed
        private readonly List<TaskInfo> _taskInfos;
        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;
        private IFailureState _failureStatus;

        private readonly object _taskLock;
        private readonly object _statusLock;

        public DefaultTaskSetManager(int numTasks, float failRatio = 0.5F)
        {
            _finalized = false;

            _contextsAdded = 0;
            _tasksAdded = 0;
            _tasksRunning = 0;
            _tasksFailed = 0;
            _numTasks = numTasks;
            _failRatio = failRatio;

            _taskInfos = new List<TaskInfo>(numTasks);
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();
            _failureStatus = new DefaultFailureState();

            _taskLock = new object();
            _statusLock = new object();

            for (int i = 0; i < numTasks; i++)
            {
                _taskInfos.Add(null);
            }
        }

        public void AddTaskSetSubscription(IElasticTaskSetSubscription subscription)
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Cannot add subscription to an already built TaskManager");
            }

            _subscriptions.Add(subscription.SubscriptionName, subscription);
        }

        public bool HasMoreContextToAdd()
        {
             return _contextsAdded < _numTasks;
        }

        public int GetNextTaskContextId(IAllocatedEvaluator evaluator = null)
        {
            if (_contextsAdded > _numTasks)
            {
                throw new IllegalStateException("Trying to schedule too many contextes");
            }

            return Interlocked.Increment(ref _contextsAdded);
        }

        public string SubscriptionsId
        {
            get
            {
                if (_finalized != true)
                {
                    throw new IllegalStateException("Task set have to be built before getting its subscriptions");
                }

                return _subscriptions.Keys.Aggregate((current, next) => current + "+" + next);
            }
        }

        public int GetNextTaskId(IActiveContext context = null)
        {
            return Utils.GetContextNum(context);
        }

        public IEnumerable<IElasticTaskSetSubscription> IsMasterTaskContext(IActiveContext activeContext)
        {
            return _subscriptions.Values.Where(sub => sub.IsMasterTaskContext(activeContext));
        }

        public void AddTask(string taskId, IConfiguration partialTaskConfig, IActiveContext activeContext)
        {
            if (_finalized != true)
            {
                throw new IllegalStateException("Task set have to be built before adding tasks");
            }

            int id = Utils.GetTaskNum(taskId) - 1;

            if (_taskInfos[id] != null)
            {
                throw new ArgumentException("Task Id already registered with TaskSet");
            }

            Interlocked.Increment(ref _tasksAdded);
            var subList = new List<IElasticTaskSetSubscription>();

            foreach (var sub in _subscriptions)
            {
                if (sub.Value.AddTask(taskId))
                {
                    subList.Add(sub.Value);
                }
            }

            _taskInfos[id] = new TaskInfo(partialTaskConfig, activeContext, TaskStatus.Init, subList);

            if (StartSubmitTasks())
            {
                SubmitTasks();
            }
        }

        public bool StartSubmitTasks()
        {
            var canI = _subscriptions.All(sub => sub.Value.ScheduleSubscription());

            return _tasksAdded == _numTasks && canI;
        }

        public void SubmitTasks()
        {
            for (int i = 0; i < _numTasks; i++)
            {
                var subs = _taskInfos[i].Subscriptions;
                ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

                foreach (var sub in subs)
                {
                    ICsConfigurationBuilder confSubBuilder = TangFactory.GetTang().NewConfigurationBuilder();

                    sub.GetTaskConfiguration(ref confSubBuilder, i + 1);

                   _subscriptions.Values.First().Service.SerializeSubscriptionConfiguration(ref confBuilder, confSubBuilder.Build());
                }

                IConfiguration serviceConf = _subscriptions.Values.First().Service.GetTaskConfiguration(confBuilder);

                IConfiguration mergedTaskConf = Configurations.Merge(_taskInfos[i].TaskConfiguration, serviceConf);

                _taskInfos[i].ActiveContext.SubmitTask(mergedTaskConf);

                _taskInfos[i].TaskStatus = TaskStatus.Submitted;
            }
        }

        public ITaskSetManager Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("TaskManager cannot be built more than once");
            }

            _finalized = true;

            return this;
        }

        public void OnTaskRunning(IRunningTask task)
        {
            if (BelongsTo(task.Id))
            {
                Interlocked.Increment(ref _tasksRunning);
                var id = Utils.GetTaskNum(task.Id) - 1;

                lock (_taskLock)
                {
                    if (_taskInfos[id].TaskStatus <= TaskStatus.Submitted)
                    {
                        _taskInfos[id].TaskStatus = TaskStatus.Running;
                        _taskInfos[id].TaskRunner = task;
                    }
                }
            }
        }

        public void OnTaskCompleted(ICompletedTask taskInfo)
        {
            if (BelongsTo(taskInfo.Id))
            {
                Interlocked.Decrement(ref _tasksRunning);
                var id = Utils.GetTaskNum(taskInfo.Id) - 1;

                lock (_taskLock)
                {
                    _taskInfos[id].TaskStatus = TaskStatus.Completed;
                    _taskInfos[id].TaskRunner.Dispose();
                    _taskInfos[id].TaskRunner = null;
                }
            }
        }

        public void OnTaskMessage(ITaskMessage message)
        {
            IEnumerable<DriverMessage> returnMessages = new List<DriverMessage>();

            foreach (var sub in _subscriptions.Values)
            {
                returnMessages = returnMessages.Concat(sub.OnTaskMessage(message));
            }

            SendToTasks(returnMessages);
        }

        /// <summary>
        /// A task set is done when we have no more tasks running and the failure state is
        /// not stop and reschedule
        /// </summary>
        public bool Done()
        {
            return _tasksRunning == 0
                && !_taskInfos.Any(info => info.TaskStatus < TaskStatus.Failed)
                && (DefaultFailureStates)_failureStatus.FailureState < DefaultFailureStates.StopAndReschedule;
        }

        public IFailureState OnTaskFailure(IFailedTask info)
        {
            if (BelongsTo(info.Id))
            {
                var id = Utils.GetTaskNum(info.Id) - 1;
                Interlocked.Decrement(ref _tasksRunning);
                Interlocked.Increment(ref _tasksFailed);

                if (_tasksFailed / _numTasks > _failRatio)
                {
                    LOGGER.Log(Level.Info, "Failure " + info.Message + " triggered a fail event");
                    OnFail();
                    return new FailState();
                }

                lock (_taskLock)
                {
                    if (_taskInfos[id].TaskStatus < TaskStatus.Failed)
                    {
                        _taskInfos[id].TaskStatus = TaskStatus.Failed;
                    }
                }

                if (info.AsError() is OperatorException)
                {
                    IFailureState currentState = new DefaultFailureState();

                    foreach (IElasticTaskSetSubscription sub in _taskInfos[id].Subscriptions)
                    {
                        currentState = currentState.Merge(sub.OnTaskFailure(info));
                    }

                    lock (_statusLock)
                    {
                        _failureStatus = currentState;
                    }

                    if (_failureStatus.FailureState > (int)DefaultFailureStates.Continue)
                    {
                        IEnumerable<DriverMessage> messages = null;

                        switch ((DefaultFailureStateEvents)_failureStatus.FailureState)
                        {
                            case DefaultFailureStateEvents.Reconfigure:
                                LOGGER.Log(Level.Info, "Failure on " + info.Id + " triggered a reconfiguration event");
                                IReconfigure reconfigureEvent = new ReconfigureEvent(info);
                                messages = EventDispatcher(reconfigureEvent);
                                break;
                            case DefaultFailureStateEvents.Reschedule:
                                LOGGER.Log(Level.Info, "Failure on " + info.Id + " triggered a reschedule event");
                                IReschedule rescheduleEvent = new RescheduleEvent();
                                messages = EventDispatcher(rescheduleEvent);
                                break;
                            case DefaultFailureStateEvents.Stop:
                                LOGGER.Log(Level.Info, "Failure on " + info.Id + " triggered a stop event");
                                IStop stopEvent = new StopEvent();
                                messages = EventDispatcher(stopEvent);
                                break;
                            default:
                                OnFail();
                                break;
                        }

                        SendToTasks(messages);
                    }
                }
                else
                {
                    OnFail();
                }
            }
   
            return null;
        }

        public void OnEvaluatorFailure(IFailedEvaluator evaluator)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<DriverMessage> EventDispatcher(IFailureEvent @event)
        {
            IEnumerable<DriverMessage> messages = new List<DriverMessage>();

            foreach (IElasticTaskSetSubscription sub in _subscriptions.Values)
            {
                messages = messages.Concat(sub.EventDispatcher(@event));
            }

            switch ((DefaultFailureStateEvents)@event.FailureEvent)
            {
                case DefaultFailureStateEvents.Reconfigure:
                    messages = messages.Concat(OnReconfigure(@event as IReconfigure));
                    break;
                case DefaultFailureStateEvents.Reschedule:
                    messages = messages.Concat(OnReschedule(@event as IReschedule));
                    break;
                case DefaultFailureStateEvents.Stop:
                    messages = messages.Concat(OnStop(@event as IStop));
                    break;
                default:
                    break;
            }

            return messages;
        }

        public IList<DriverMessage> OnReconfigure(IReconfigure info)
        {
            LOGGER.Log(Level.Info, "Reconfiguring the task set manager");
            return new List<DriverMessage>();
        }

        public IList<DriverMessage> OnReschedule(IReschedule rescheduleEvent)
        {
            LOGGER.Log(Level.Info, "Going to reschedule a task");
            return new List<DriverMessage>();
        }

        public IList<DriverMessage> OnStop(IStop stopEvent)
        {
            LOGGER.Log(Level.Info, "Going to stop the execution and reschedule a task");
            return new List<DriverMessage>();
        }

        public void OnFail()
        {
            Dispose();
        }

        public void Dispose()
        {
            lock (_taskLock)
            {
                foreach (var info in _taskInfos)
                {
                    if (info != null)
                    {
                        info.Dispose();
                    }
                }
            }
        }

        private bool BelongsTo(string id)
        {
            return Utils.GetTaskSubscriptions(id) == SubscriptionsId;
        }

        private void SendToTasks(IEnumerable<DriverMessage> messages)
        {
            foreach (var returnMessage in messages)
            {
                if (returnMessage != null)
                {
                    var destination = Utils.GetTaskNum(returnMessage.Destination) - 1;
                    if (_taskInfos[destination] == null)
                    {
                        throw new ArgumentNullException("Task Info");
                    }
                    if (_taskInfos[destination].TaskRunner == null)
                    {
                        throw new ArgumentNullException("Task Runner");
                    }

                    _taskInfos[destination].TaskRunner.Send(returnMessage.Serialize());
                }
            }
        }
    }
}
