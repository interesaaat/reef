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
using Org.Apache.REEF.Network.Elastic.Comm;

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
        private readonly int _numTasks;

        // Task info 0-indexed
        private readonly List<TaskInfo> _taskInfos;
        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;
        private IFailureState _failureStatus;

        private readonly object _taskLock;
        private readonly object _statusLock;

        public DefaultTaskSetManager(int numTasks)
        {
            _finalized = false;

            _contextsAdded = 0;
            _tasksAdded = 0;
            _tasksRunning = 0;
            _numTasks = numTasks;

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

        public string GetNextTaskContextId(IAllocatedEvaluator evaluator)
        {
            if (_contextsAdded > _numTasks)
            {
                throw new IllegalStateException("Trying to schedule too many contextes");
            }

            int id = Interlocked.Increment(ref _contextsAdded);
            return Utils.BuildTaskId(SubscriptionsId, id);
        }

        public string GetNextTaskId(IActiveContext context)
        {
            var id = Utils.GetContextNum(context);
            return Utils.BuildTaskId(SubscriptionsId, id);
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
            System.Threading.Thread.Sleep(30000);

            for (int i = 0; i < _numTasks; i++)
            {
                SubmitTask(i);
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
                    if (_taskInfos[id].TaskStatus == TaskStatus.Completed)
                    {
                        LOGGER.Log(Level.Info, "Received running from task {0} which is completed: ignoring");
                        return;
                    }

                    if (_taskInfos[id].TaskStatus != TaskStatus.Running)
                    {
                        if (_taskInfos[id].TaskStatus == TaskStatus.Resubmitted)
                        {
                            foreach (var sub in _subscriptions)
                            {
                                sub.Value.AddTask(task.Id);
                            }
                        }

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
            var returnMessages = new List<IElasticDriverMessage>();

            foreach (var sub in _subscriptions.Values)
            {
                sub.OnTaskMessage(message, ref returnMessages);
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

        public void OnTaskFailure(IFailedTask info)
        {
            var failureEvents = new List<IFailureEvent>();
            OnTaskFailure(info, ref failureEvents);
        }

        public void OnTaskFailure(IFailedTask info, ref List<IFailureEvent> failureEvents)
        {
            if (BelongsTo(info.Id))
            {
                var id = Utils.GetTaskNum(info.Id) - 1;
                Interlocked.Decrement(ref _tasksRunning);

                lock (_taskLock)
                {
                    if (_taskInfos[id].TaskStatus < TaskStatus.Failed)
                    {
                        _taskInfos[id].TaskStatus = TaskStatus.Failed;
                    }
                }

                if (info.AsError() is OperatorException)
                {
                    failureEvents = failureEvents ?? new List<IFailureEvent>();

                    LOGGER.Log(Level.Info, "Received an Operator Exception from Task " + info.Id, info.AsError());

                    foreach (IElasticTaskSetSubscription sub in _taskInfos[id].Subscriptions)
                    {
                       sub.OnTaskFailure(info, ref failureEvents);
                    }

                    // Failures have to be propagated up to the service
                    _taskInfos[id].Subscriptions.First().Service.OnTaskFailure(info, ref failureEvents);

                    if (failureEvents.Any(ev => ev.FailureEvent == (int)DefaultFailureStateEvents.Fail))
                    {
                        OnFail(info.Id);
                    }
                    else
                    {
                        for (int i = 0; i < failureEvents.Count; i++)
                        {
                            var @event = failureEvents[i];
                            EventDispatcher(ref @event);
                        }
                    }  
                }
                else
                {
                    LOGGER.Log(Level.Info, "Failure " + info.Message + " triggered a fail event");

                    OnFail(info.Id);
                }
            }
        }

        public void OnEvaluatorFailure(IFailedEvaluator evaluator)
        {
            throw new NotImplementedException();
        }

        public void EventDispatcher(ref IFailureEvent @event)
        {
            var id = Utils.GetTaskNum(@event.TaskId) - 1;

            _taskInfos[id].Subscriptions.First().Service.EventDispatcher(ref @event);

            foreach (IElasticTaskSetSubscription sub in _taskInfos[id].Subscriptions)
            {
                sub.EventDispatcher(ref @event);
            }

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
                    break;
            }
        }

        public void OnReconfigure(ref IReconfigure reconfigureEvent)
        {
            lock (_statusLock)
            {
                _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReconfigure));
            }

            SendToTasks(reconfigureEvent.FailureResponse);
        }

        public void OnReschedule(ref IReschedule rescheduleEvent)
        {
            lock (_statusLock)
            {
                _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReschedule));
            }

            SendToTasks(rescheduleEvent.FailureResponse);

            var id = Utils.GetTaskNum(rescheduleEvent.TaskId) - 1;
            _taskInfos[id].NumRetry++;

            if (_taskInfos[id].TaskRunner != null)
            {
                _taskInfos[id].TaskRunner.Dispose();
                _taskInfos[id].TaskRunner = null;

                var resubmitConf = rescheduleEvent.TaskConfigurations.ToArray();

                // When we resubmit the task need to be reconfigured
                // If there is no reconfiguration, the task doesn't need to be rescheduled
                if (resubmitConf.Length > 0)
                {
                    LOGGER.Log(Level.Info, "Going to reschedule task {0}", rescheduleEvent.TaskId);

                    SubmitTask(id, rescheduleEvent.TaskConfigurations.ToArray());
                }
            }
        }

        public void OnStop(ref IStop stopEvent)
        {
            lock (_statusLock)
            {
                _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.StopAndReschedule));
            }

            SendToTasks(stopEvent.FailureResponse);
        }

        public void OnFail(string taskId)
        {
            LOGGER.Log(Level.Info, "Task set failed because of failure in Task {0}", taskId);

            lock (_statusLock)
            {
                _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.Fail));
            }

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

        private void SubmitTask(int id, params IConfiguration[] additonalConfs)
        {
            var subs = _taskInfos[id].Subscriptions;
            ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            foreach (var sub in subs)
            {
                ICsConfigurationBuilder confSubBuilder = TangFactory.GetTang().NewConfigurationBuilder();

                sub.GetTaskConfiguration(ref confSubBuilder, id + 1);

                var confSub = confSubBuilder.Build();

                foreach (var additionalConf in additonalConfs)
                {
                    confSub = Configurations.Merge(confSub, additionalConf);
                }

                _subscriptions.Values.First().Service.SerializeSubscriptionConfiguration(ref confBuilder, confSub);
            }

            IConfiguration serviceConf = _subscriptions.Values.First().Service.GetTaskConfiguration(confBuilder);

            IConfiguration mergedTaskConf = Configurations.Merge(_taskInfos[id].TaskConfiguration, serviceConf);

            _taskInfos[id].ActiveContext.SubmitTask(mergedTaskConf);

            if (_taskInfos[id].TaskStatus == TaskStatus.Failed)
            {
                _taskInfos[id].TaskStatus = TaskStatus.Resubmitted;
            }
            else
            {
                _taskInfos[id].TaskStatus = TaskStatus.Submitted;
            }
        }

        private bool BelongsTo(string id)
        {
            return Utils.GetTaskSubscriptions(id) == SubscriptionsId;
        }

        private void SendToTasks(IList<IElasticDriverMessage> messages)
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
                    if (_taskInfos[destination].TaskStatus != TaskStatus.Running  || 
                        _taskInfos[destination].TaskRunner == null)
                    {
                        var msg = "Cannot send message type " + returnMessage.Message.MessageType;
                        msg += " to " + (destination + 1) + ": failing";

                        throw new IllegalStateException(msg);
                    }

                    _taskInfos[destination].TaskRunner.Send(returnMessage.Serialize());
                }
            }
        }
    }
}
