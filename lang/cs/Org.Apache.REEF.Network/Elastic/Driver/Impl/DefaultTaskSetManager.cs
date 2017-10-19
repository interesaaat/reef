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
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public sealed class DefaultTaskSetManager : 
        ITaskSetManager, 
        IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskSetManager));

        private bool _finalized;
        private bool _disposed;
        private readonly TaskSetManagerParameters _parameters;

        private int _contextsAdded;
        private int _tasksAdded;
        private int _tasksRunning;
        private readonly int _numTasks;

        // Task info 0-indexed
        private readonly List<TaskInfo> _taskInfos;
        private readonly ConcurrentDictionary<string, IAllocatedEvaluator> _evaluators;
        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;
        private IFailureState _failureStatus;

        private readonly object _taskLock;
        private readonly object _statusLock;

        public DefaultTaskSetManager(int numTasks, params IConfiguration[] confs)
        {
            _finalized = false;
            _disposed = false;

            _contextsAdded = 0;
            _tasksAdded = 0;
            _tasksRunning = 0;
            _numTasks = numTasks;

            _taskInfos = new List<TaskInfo>(numTasks);
            _evaluators = new ConcurrentDictionary<string, IAllocatedEvaluator>();
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();
            _failureStatus = new DefaultFailureState();

            _taskLock = new object();
            _statusLock = new object();

            for (int i = 0; i < numTasks; i++)
            {
                _taskInfos.Add(null);
            }

            var injector = TangFactory.GetTang().NewInjector(confs);
            Type parametersType = typeof(TaskSetManagerParameters);
            _parameters = injector.GetInstance(parametersType) as TaskSetManagerParameters;
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
                throw new IllegalStateException("Trying to schedule too many contexts");
            }

            if (!_evaluators.ContainsKey(evaluator.Id))
            {
                _evaluators.TryAdd(evaluator.Id, evaluator);
            }

            int id = Interlocked.Increment(ref _contextsAdded);
            return Utils.BuildContextId(SubscriptionsId, id);
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

            if (Completed())
            {
                LOGGER.Log(Level.Warning, "Adding task to already completed Task Set: ignoring");
                return;
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
            ////System.Threading.Thread.Sleep(30000);
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
                        if (_taskInfos[id].TaskStatus == TaskStatus.Recovering)
                        {
                            foreach (var sub in _subscriptions)
                            {
                                sub.Value.AddTask(task.Id);
                            }
                        }

                        _taskInfos[id].TaskStatus = TaskStatus.Running;
                        _taskInfos[id].TaskRunner = task;
                        Interlocked.Increment(ref _tasksRunning);
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
                    _taskInfos[id].TaskRunner = null;

                    if (Completed())
                    {
                        foreach (var info in _taskInfos.Where(info => info.TaskRunner != null))
                        {
                            info.TaskRunner.Dispose();
                            info.TaskRunner = null;
                        }
                    }
                }
            }
        }

        public void OnTaskMessage(ITaskMessage message)
        {
            if (BelongsTo(message.TaskId))
            {
                var id = Utils.GetTaskNum(message.TaskId) - 1;

                lock (_taskLock)
                {
                    var returnMessages = new List<IElasticDriverMessage>();

                    foreach (var sub in _subscriptions.Values)
                    {
                        sub.OnTaskMessage(message, ref returnMessages);
                    }

                    SendToTasks(returnMessages);
                }
            }
        }

        public bool Completed()
        {
            return _subscriptions.Select(sub => sub.Value.Completed).Aggregate((com1, com2) => com1 && com2);
        }

        public bool Failed()
        {
            return _failureStatus.FailureState == (int)DefaultFailureStates.Fail;
        }

        /// <summary>
        /// A task set is done when all subscriptions are completed ore we have no more tasks running
        /// </summary>
        public bool Done()
        {
            return Completed() && _tasksRunning == 0;
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
                Interlocked.Decrement(ref _tasksRunning);

                if (Completed())
                {
                    LOGGER.Log(Level.Info, "Received a Task failure but Task Manager is complete: ignoring the failure " + info.Id, info.AsError());

                    return;
                }

                var id = Utils.GetTaskNum(info.Id) - 1;

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

                    for (int i = 0; i < failureEvents.Count; i++)
                    {
                        var @event = failureEvents[i];
                        EventDispatcher(ref @event);
                    }
                }
                else
                {
                    LOGGER.Log(Level.Info, "Failure " + info.Message + " triggered a fail event");

                    OnFail();
                }
            }
        }

        public void OnEvaluatorFailure(IFailedEvaluator evaluator)
        {
            Console.WriteLine("Failed Evaluator {0} with error {1}", evaluator.Id, evaluator.EvaluatorException);
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
                    OnFail();
                    break;
            }
        }

        public void OnReconfigure(ref IReconfigure reconfigureEvent)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReconfigure));
            }

            SendToTasks(reconfigureEvent.FailureResponse);
        }

        public void OnReschedule(ref IReschedule rescheduleEvent)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReschedule));
            }

            SendToTasks(rescheduleEvent.FailureResponse);

            lock (_taskLock)
            {
                var id = Utils.GetTaskNum(rescheduleEvent.TaskId) - 1;
                _taskInfos[id].NumRetry++;

                if (_taskInfos[id].NumRetry > _parameters.Retry)
                {
                    LOGGER.Log(Level.Error, "Task {0} failed more than {0} times: Aborting", _parameters.Retry);
                    OnFail();
                }

                if (_taskInfos[id].TaskRunner != null)
                {
                    _taskInfos[id].TaskRunner.Dispose();
                    _taskInfos[id].TaskRunner = null;

                    var resubmitConf = rescheduleEvent.TaskConfigurations.ToArray();

                    // When we resubmit the task need to be reconfigured
                    // If there is no reconfiguration, the task doesn't need to be rescheduled
                    if (resubmitConf.Length > 0)
                    {
                        LOGGER.Log(Level.Info, "Rescheduling task {0}", rescheduleEvent.TaskId);

                        SubmitTask(id, rescheduleEvent.TaskConfigurations.ToArray());
                    }
                }
            }
        }

        public void OnStop(ref IStop stopEvent)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.StopAndReschedule));
            }

            SendToTasks(stopEvent.FailureResponse);
        }

        public void OnFail()
        {
            LOGGER.Log(Level.Info, "Task set failed");

            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.Fail));
            }

            Dispose();
        }

        public void Dispose()
        {
            lock (_taskLock)
            {
                if (!_disposed)
                {
                    foreach (var info in _taskInfos)
                    {
                        if (info != null)
                        {
                            info.Dispose();
                        }
                    }

                    _disposed = true;
                }
            }
        }

        private void SubmitTask(int id, params IConfiguration[] additonalConfs)
        {
            if (Completed() || Failed())
            {
                LOGGER.Log(Level.Warning, "Task submit for a completed or failed Task Set: ignoring");
                _taskInfos[id].Dispose();
                return;
            }

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

            lock (_taskLock)
            {
                if (_taskInfos[id].ActiveContext == null)
                {
                    LOGGER.Log(Level.Warning, "Task submit for a non-active context: ignoring");
                    return;
                }

                _taskInfos[id].ActiveContext.SubmitTask(mergedTaskConf);

                if (_taskInfos[id].TaskStatus == TaskStatus.Failed)
                {
                    _taskInfos[id].TaskStatus = TaskStatus.Recovering;
                }
                else
                {
                    _taskInfos[id].TaskStatus = TaskStatus.Submitted;
                }
            }
        }

        private bool BelongsTo(string id)
        {
            return Utils.GetTaskSubscriptions(id) == SubscriptionsId;
        }

        private void SendToTasks(IList<IElasticDriverMessage> messages, int retry = 0)
        {
            foreach (var returnMessage in messages)
            {
                if (returnMessage != null)
                {
                    var destination = Utils.GetTaskNum(returnMessage.Destination) - 1;

                    lock (_taskLock)
                    {
                        if (_taskInfos[destination] == null)
                        {
                            throw new ArgumentNullException("Task Info");
                        }
                        if (Completed() || Failed())
                        {
                            LOGGER.Log(Level.Warning, "Task submit for a completed or failed Task Set: ignoring");
                            _taskInfos[destination].Dispose();

                            return;
                        }
                        if (_taskInfos[destination].TaskStatus != TaskStatus.Running ||
                            _taskInfos[destination].TaskRunner == null)
                        {
                            var msg = "Cannot send message to ";
                            msg += (destination + 1) + ": Task Status is " + _taskInfos[destination].TaskStatus;

                            if (_taskInfos[destination].TaskStatus == TaskStatus.Submitted && retry < _parameters.Retry)
                            {
                                LOGGER.Log(Level.Warning, msg + "\nRetry");
                                System.Threading.Tasks.Task.Run(() => SendToTasks(new List<IElasticDriverMessage>() { returnMessage }, retry + 1));
                            }
                            else if (retry >= _parameters.Retry)
                            {
                                LOGGER.Log(Level.Warning, msg + "\nAborting");
                                OnFail();
                            }
                            else
                            {
                                LOGGER.Log(Level.Warning, msg + "\nIgnoring");
                            }

                            ////else if (_taskInfos[destination].ActiveContext != null)
                            ////{
                            ////    LOGGER.Log(Level.Warning, msg + "\nGoing to kill and resubmit the context");
                            ////    var evaluatorId = _taskInfos[destination].ActiveContext.EvaluatorId;
                            ////    var contextId = _taskInfos[destination].ActiveContext.Id;
                            ////    _taskInfos[destination].ActiveContext.Dispose();

                            ////    // Eventually will do the resubmit
                            ////}
                            return;
                        }

                        _taskInfos[destination].TaskRunner.Send(returnMessage.Serialize());
                    }
                }
            }
        }
    }
}
