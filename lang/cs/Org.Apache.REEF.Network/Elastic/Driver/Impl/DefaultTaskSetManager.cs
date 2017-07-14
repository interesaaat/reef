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

        private readonly object _lock;
        private bool _finalized;

        private int _contextsAdded;
        private int _tasksAdded;
        private readonly int _numTasks;
        private int _tasksFailed;
        private float _failRatio;

        private readonly List<TaskInfo> _taskInfos; 
        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;

        public DefaultTaskSetManager(int numTasks, float failRatio = 0.5F)
        {
            _lock = new object();
            _finalized = false;

            _contextsAdded = 0;
            _tasksAdded = 0;
            _numTasks = numTasks;
            _failRatio = failRatio;
            _tasksFailed = 0;

            _taskInfos = new List<TaskInfo>(numTasks);
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();

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

        public bool HasMoreContextToAdd
        {
            get
            {
                return _contextsAdded < _numTasks;
            }
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

        public int NumTasks
        {
            get
            {
                return _numTasks;
            }
        }

        public void AddTask(string taskId, IConfiguration partialTaskConfig, IActiveContext activeContext)
        {
            int id = Utils.GetTaskNum(taskId) - 1;

            if (_taskInfos[id] != null)
            {
                throw new ArgumentException(
                    "Task Id already registered with TaskSet");
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

            if (StartSubmitTasks)
            {
                SubmitTasks();
            }
        }

        public bool StartSubmitTasks
        {
            get
            {
                var canI = _subscriptions.All(sub => sub.Value.ScheduleSubscription == true);

                return _tasksAdded == _numTasks && canI;
            }
        }

        public void SubmitTasks()
        {
            for (int i = 0; i < _numTasks; i++)
            {
                var subs = _taskInfos[i].Subscriptions;
                ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

                foreach (var sub in subs)
                {
                    sub.GetTaskConfiguration(ref confBuilder);
                }

                IConfiguration serviceConfiguration = _subscriptions.Values.First().Service.GetTaskConfiguration(confBuilder);
                IConfiguration mergedTaskConf = Configurations.Merge(_taskInfos[i].TaskConfiguration, serviceConfiguration);

                _taskInfos[i].ActiveContext.SubmitTask(mergedTaskConf);

                _taskInfos[i].TaskStatus = TaskStatus.Submitted;
            }
        }

        public void Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("TaskManager cannot be built more than once");
            }

            _finalized = true;
        }

        public void OnTaskRunning(IRunningTask task)
        {
            if (BelongsTo(task.Id))
            {
                var id = Utils.GetTaskNum(task.Id) - 1;

                lock (_lock)
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
                var id = Utils.GetTaskNum(taskInfo.Id) - 1;

                lock (_lock)
                {
                    _taskInfos[id].TaskStatus = TaskStatus.Completed;
                    _taskInfos[id].TaskRunner.Dispose();
                }
            }
        }

        public bool Done
        {
            get
            {
                return _taskInfos.Any(info => info.TaskStatus != TaskStatus.Completed);
            }
        }

        public void Dispose()
        {
            foreach (var info in _taskInfos)
            {
                info.ActiveContext.Dispose();
            }
        }

        public IFailureState OnTaskFailure(IFailedTask info)
        {
            if (BelongsTo(info.Id))
            {
                var id = Utils.GetTaskNum(info.Id) - 1;
                Interlocked.Decrement(ref _tasksFailed);

                if (_tasksFailed / _numTasks > _failRatio)
                {
                    OnFail();
                    return new Fail();
                }

                lock (_lock)
                {
                    if (_taskInfos[id].TaskStatus < TaskStatus.Failed)
                    {
                        _taskInfos[id].TaskStatus = TaskStatus.Failed;
                    }
                }

                if (info.AsError() is OperatorException)
                {
                    foreach (IElasticTaskSetSubscription sub in _taskInfos[id].Subscriptions)
                    {
                        sub.OnTaskFailure(info);
                    }

                    IFailureState state = _subscriptions.First(pairs => true).Value.Service.OnTaskFailure(info);

                    if (state.FailureState > (int)DefaultFailureStates.Continue)
                    {
                        switch ((DefaultFailureStateEvents)state.FailureState)
                        {
                            case DefaultFailureStateEvents.Reconfigure:
                                IReconfigure reconfigureEvent = null;
                                EventDispatcher(reconfigureEvent);
                                break;
                            case DefaultFailureStateEvents.Reschedule:
                                IReschedule rescheduleEvent = null;
                                EventDispatcher(rescheduleEvent);
                                break;
                            case DefaultFailureStateEvents.Stop:
                                IStop stopEvent = null;
                                EventDispatcher(stopEvent);
                                break;
                        }
                    }
                }
            }
            return null;
        }

        public void OnEvaluatorFailure(IFailedEvaluator evaluator)
        {
            throw new NotImplementedException();
        }

        public void EventDispatcher(IFailureEvent @event)
        {
            foreach (IElasticTaskSetSubscription sub in _subscriptions.Values)
            {
                sub.EventDispatcher(@event);
            }

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
        }

        public void OnReconfigure(IReconfigure reconfigureEvent)
        {
            throw new NotImplementedException();
        }

        public void OnReschedule(IReschedule rescheduleEvent)
        {
            throw new NotImplementedException();
        }

        public void OnStop(IStop stopEvent)
        {
            throw new NotImplementedException();
        }

        public void OnFail()
        {
            throw new NotImplementedException();
        }

        private bool BelongsTo(string id)
        {
            return Utils.GetTaskSubscriptions(id) == SubscriptionsId;
        }
    }
}
