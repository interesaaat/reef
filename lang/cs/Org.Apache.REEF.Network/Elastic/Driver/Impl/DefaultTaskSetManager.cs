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
using Org.Apache.REEF.Wake.Time.Event;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public sealed class DefaultTaskSetManager : 
        ITaskSetManager, 
        IDefaultFailureEventResponse,
        IObserver<Alarm>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskSetManager));

        private bool _finalized;
        private volatile bool _disposed;
        private volatile bool _scheduled;
        private volatile bool _completed;
        private readonly TaskSetManagerParameters _parameters;

        private volatile int _contextsAdded;
        private int _tasksAdded;
        private int _tasksRunning;
        private volatile int _totFailedTasks;
        private volatile int _totFailedEvaluators;

        private readonly int _numTasks;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly Func<string, IConfiguration> _masterTaskConfiguration;
        private readonly Func<string, IConfiguration> _slaveTaskConfiguration;

        // Task info 0-indexed
        private readonly List<TaskInfo> _taskInfos;
        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;
        private readonly ConcurrentQueue<int> _queuedTasks;
        private readonly ConcurrentQueue<int> _queuedContexts;
        private IFailureState _failureStatus;
        private volatile bool _hasProgress;

        private readonly object _statusLock;

        public DefaultTaskSetManager(
            int numTasks,
            IEvaluatorRequestor evaluatorRequestor,
            Func<string, IConfiguration> masterTaskConfiguration, 
            Func<string, IConfiguration> slaveTaskConfiguration = null, 
            params IConfiguration[] confs)
        {
            _finalized = false;
            _scheduled = false;
            _disposed = false;
            _completed = false;

            _contextsAdded = 0;
            _tasksAdded = 0;
            _tasksRunning = 0;
            _totFailedTasks = 0;
            _totFailedEvaluators = 0;

            _numTasks = numTasks;
            _evaluatorRequestor = evaluatorRequestor;
            _masterTaskConfiguration = masterTaskConfiguration;
            _slaveTaskConfiguration = slaveTaskConfiguration ?? masterTaskConfiguration;

            _taskInfos = new List<TaskInfo>(numTasks);
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();
            _queuedTasks = new ConcurrentQueue<int>();
            _queuedContexts = new ConcurrentQueue<int>();
            _failureStatus = new DefaultFailureState();
            _hasProgress = true;

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
            int id;

            if (_queuedTasks.TryDequeue(out id))
            {
                return Utils.BuildContextId(SubscriptionsId, id);
            }

            if (_queuedContexts.TryDequeue(out id))
            {
                return Utils.BuildContextId(SubscriptionsId, id);
            }

            id = Interlocked.Increment(ref _contextsAdded);

            if (_contextsAdded > _numTasks)
            {
                throw new IllegalStateException("Trying to schedule too many contexts");
            }

            return Utils.BuildContextId(SubscriptionsId, id);
        }

        public string GetTaskId(IActiveContext context)
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

        public void OnNewActiveContext(IActiveContext activeContext)
        {
            if (_finalized != true)
            {
                throw new IllegalStateException("Task set have to be built before adding tasks");
            }

            if (Completed() || Failed())
            {
                LOGGER.Log(Level.Warning, "Adding tasks to already completed Task Set: ignoring");
                activeContext.Dispose();
                return;
            }

            var id = Utils.GetContextNum(activeContext) - 1;

            if (_taskInfos[id] != null)
            {
                LOGGER.Log(Level.Info, "{0} already part of Task Set: going to directly submit it", Utils.BuildTaskId(SubscriptionsId, id + 1));

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].UpdateRuntime(activeContext, activeContext.EvaluatorId);
                }

                SubmitTask(id);
            }
            else
            {
                bool isMaster = IsMasterTaskContext(activeContext).Any();
                var taskId = Utils.BuildTaskId(SubscriptionsId, id + 1);

                IConfiguration partialTaskConf;

                if (isMaster)
                {
                    partialTaskConf = _masterTaskConfiguration(taskId);
                }
                else
                {
                    partialTaskConf = _slaveTaskConfiguration(taskId);
                }

                AddTask(taskId, partialTaskConf, activeContext);
            }
        }

        public bool StartSubmitTasks()
        {
            if (_subscriptions.All(sub => sub.Value.ScheduleSubscription()))
            {
                LOGGER.Log(Level.Info, string.Format("Scheduling {0} tasks from Taskset {1}", _tasksAdded, SubscriptionsId));

                _scheduled = true;
            }
            return _scheduled;
        }

        public void SubmitTasks()
        {
            List<IElasticDriverMessage> msgs = null;
            var nextTimeouts = new List<Failures.Impl.Timeout>();

            OnTimeout(new TasksetAlarm(0, this), ref msgs, ref nextTimeouts);

            for (int i = 0; i < _numTasks; i++)
            {
                if (_taskInfos[i] != null)
                {
                    SubmitTask(i);
                }
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
                _hasProgress = true;

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].SetTaskRunner(task);

                    if (Completed() || Failed())
                    {
                        LOGGER.Log(Level.Info, "Received running from task {0} but Taskset is completed or failed: ignoring", task.Id);
                        _taskInfos[id].DisposeTask();

                        return;
                    }
                    if (!TaskStatusUtils.IsRunnable(_taskInfos[id].TaskStatus))
                    {
                        LOGGER.Log(Level.Info, "Received running from task {0} which is not runnable: ignoring", task.Id);
                        _taskInfos[id].DisposeTask();

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

                        _taskInfos[id].SetTaskStatus(TaskStatus.Running);
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
                _hasProgress = true;

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].SetTaskStatus(TaskStatus.Completed);
                }
                if (Completed())
                {
                    foreach (var info in _taskInfos.Where(info => info != null && info.TaskStatus < TaskStatus.Failed))
                    {
                        info.DisposeTask();
                    }
                }
            }
        }

        public void OnTaskMessage(ITaskMessage message)
        {
            if (BelongsTo(message.TaskId))
            {
                var id = Utils.GetTaskNum(message.TaskId) - 1;
                var returnMessages = new List<IElasticDriverMessage>();
                _hasProgress = true;

                foreach (var sub in _subscriptions.Values)
                {
                    sub.OnTaskMessage(message, ref returnMessages);
                }

                SendToTasks(returnMessages);
            }
        }

        public bool Completed()
        {
            if (!_completed)
            {
                _completed = _subscriptions.Select(sub => sub.Value.Completed).Aggregate((com1, com2) => com1 && com2);

                if (_completed)
                {
                    LOGGER.Log(Level.Info, "TaskSet Completed");
                }
            }

            return _completed;
        }

        public bool Failed()
        {
            return _failureStatus.FailureState == (int)DefaultFailureStates.Fail;
        }

        public bool IsDone()
        {
            return Completed() && _tasksRunning == 0;
        }

        public void OnTaskFailure(IFailedTask info)
        {
            var failureEvents = new List<IFailureEvent>();

            OnTaskFailure(info, ref failureEvents);
        }

        public void OnTimeout(Alarm alarm, ref List<IElasticDriverMessage> msgs, ref List<Failures.Impl.Timeout> nextTimeouts)
        {
            var isInit = msgs == null;

            // Taskset is just started, init the timeouts
            if (isInit)
            {
                _hasProgress = false;
                LOGGER.Log(Level.Info, "Timeout alarm for Taskset initialized");
                ////nextTimeouts.Add(new Failures.Impl.Timeout(_parameters.Timeout, this, Failures.Impl.Timeout.TimeoutType.Taskset));

                foreach (var sub in _subscriptions.Values)
                {
                    sub.OnTimeout(alarm, ref msgs, ref nextTimeouts);
                }
            }
            else if (alarm.GetType() == typeof(TasksetAlarm))
            {
                if (!_hasProgress)
                {
                    LOGGER.Log(Level.Error, "Taskset made no progress in the last {0}ms. Aborting.", _parameters.Timeout);
                    OnFail();
                    return;
                }
                else
                {
                    _hasProgress = false;
                    nextTimeouts.Add(new Failures.Impl.Timeout(_parameters.Timeout, this, Failures.Impl.Timeout.TimeoutType.Taskset));
                }
            }
            else
            {
                foreach (var sub in _subscriptions.Values)
                {
                    sub.OnTimeout(alarm, ref msgs, ref nextTimeouts);
                }

                SendToTasks(msgs);
            }

            foreach (var timeout in nextTimeouts)
            {
                _parameters.ScheduleAlarm(timeout);
            }
        }

        public void OnNext(Alarm value)
        {
            if (!Completed() && !Failed())
            {
                var msgs = new List<IElasticDriverMessage>();
                var nextTimeouts = new List<Failures.Impl.Timeout>();

                OnTimeout(value, ref msgs, ref nextTimeouts);
            }
        }

        public void OnTaskFailure(IFailedTask info, ref List<IFailureEvent> failureEvents)
        {
            if (BelongsTo(info.Id))
            {
                LOGGER.Log(Level.Info, "Received a failure from " + info.Id, info.AsError());

                Interlocked.Decrement(ref _tasksRunning);
                _totFailedTasks++;
                _hasProgress = true;
                var id = Utils.GetTaskNum(info.Id) - 1;

                if (Completed() || Failed())
                {
                    LOGGER.Log(Level.Info, "Received a Task failure but Task Manager is completed or failed: ignoring the failure " + info.Id, info.AsError());

                    lock (_taskInfos[id].Lock)
                    {
                        _taskInfos[id].SetTaskStatus(TaskStatus.Failed);
                    }

                    return;
                }

                failureEvents = failureEvents ?? new List<IFailureEvent>();

                lock (_taskInfos[id].Lock)
                {
                    if (_taskInfos[id].TaskStatus < TaskStatus.Failed)
                    {
                        _taskInfos[id].SetTaskStatus(TaskStatus.Failed);
                    }

                    foreach (IElasticTaskSetSubscription sub in _taskInfos[id].Subscriptions)
                    {
                        sub.OnTaskFailure(info, ref failureEvents);
                    }

                    // Failures have to be propagated up to the service
                    _taskInfos[id].Subscriptions.First().Service.OnTaskFailure(info, ref failureEvents);
                }

                for (int i = 0; i < failureEvents.Count; i++)
                {
                    var @event = failureEvents[i];
                    EventDispatcher(ref @event);
                }
            }
        }

        public void OnEvaluatorFailure(IFailedEvaluator evaluator)
        {
            LOGGER.Log(Level.Info, "Received a failure from " + evaluator.Id, evaluator.EvaluatorException);
            _totFailedEvaluators++;

            if (evaluator.FailedTask.IsPresent())
            {
                var failedTask = evaluator.FailedTask.Value;
                var id = Utils.GetTaskNum(failedTask.Id) - 1;

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].DropRuntime();
                }

                OnTaskFailure(failedTask);
            }
            else
            {
                _hasProgress = true;

                if (!Completed() && !Failed())
                {
                    var found = false;
                    for (int i = 0; i < evaluator.FailedContexts.Count && !found; i++)
                    {
                        if (evaluator.FailedContexts[i].ParentContext.IsPresent())
                        {
                            var id = Utils.GetContextNum(evaluator.FailedContexts[i].ParentContext.Value);
                            _queuedContexts.Enqueue(id);
                            found = true;
                        }
                    }
                    SpawnNewEvaluator();
                }
            }
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
                case DefaultFailureStateEvents.Fail:
                    OnFail();
                    break;
                default:
                    throw new IllegalStateException("Failure event not recognized");
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

            var id = Utils.GetTaskNum(rescheduleEvent.TaskId) - 1;

            lock (_taskInfos[id].Lock)
            {
                _taskInfos[id].NumRetry++;

                if (_taskInfos[id].NumRetry > _parameters.NumTaskFailures)
                {
                    LOGGER.Log(Level.Error, "Task {0} failed more than {1} times: Aborting", rescheduleEvent.TaskId, _parameters.NumTaskFailures);
                    OnFail();
                }

                if (rescheduleEvent.Reschedule)
                {
                    LOGGER.Log(Level.Info, "Rescheduling task {0}", rescheduleEvent.TaskId);

                    _taskInfos[id].RescheduleConfigurations = rescheduleEvent.RescheduleTaskConfigurations;

                    SubmitTask(id);
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
            LOGGER.Log(Level.Info, "TaskSet failed");

            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.Fail));
            }

            Dispose();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                LogFinalStatistics();

                foreach (var info in _taskInfos)
                {
                    if (info != null)
                    {
                        lock (info.Lock)
                        {
                            info.Dispose();
                        }
                    }
                }
            }
        }

        private void AddTask(string taskId, IConfiguration partialTaskConfig, IActiveContext activeContext)
        {
            Interlocked.Increment(ref _tasksAdded);
            var subList = new List<IElasticTaskSetSubscription>();
            var id = Utils.GetTaskNum(taskId) - 1;
            var partitionsConfs = new List<IConfiguration>() { partialTaskConfig };

            foreach (var sub in _subscriptions)
            {
                if (sub.Value.AddTask(taskId))
                {
                    subList.Add(sub.Value);
                    var partitionConf = sub.Value.GetPartitionConf(taskId);

                    if (partitionConf.IsPresent())
                    {
                        partitionsConfs.Add(partitionConf.Value);
                    }
                }
                else
                {
                    LOGGER.Log(Level.Warning, taskId + " cannot be added to subscription " + sub.Key);
                    activeContext.Dispose();
                    return;
                }
            }

            var aggregatedConfs = partitionsConfs.Aggregate((x, y) => Configurations.Merge(x, y));

            _taskInfos[id] = new TaskInfo(aggregatedConfs, activeContext, activeContext.EvaluatorId, TaskStatus.Init, subList);

            if (_scheduled)
            {
                SubmitTask(id);
            }
            else if (StartSubmitTasks())
            {
                SubmitTasks();
            }
        }

        private void SubmitTask(int id)
        {
            if (Completed() || Failed())
            {
                LOGGER.Log(Level.Warning, "Task submit for a completed or failed Task Set: ignoring");
                _taskInfos[id].DisposeTask();

                return;
            }

            lock (_taskInfos[id].Lock)
            {
                var subs = _taskInfos[id].Subscriptions;
                ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();
                var rescheduleConfs = _taskInfos[id].RescheduleConfigurations;

                foreach (var sub in subs)
                {
                    ICsConfigurationBuilder confSubBuilder = TangFactory.GetTang().NewConfigurationBuilder();
                    var confSub = sub.GetTaskConfiguration(ref confSubBuilder, id + 1);

                    if (rescheduleConfs.TryGetValue(sub.SubscriptionName, out var confs))
                    {
                        foreach (var additionalConf in confs)
                        {
                            confSub = Configurations.Merge(confSub, additionalConf);
                        }
                    }

                    _subscriptions.Values.First().Service.SerializeSubscriptionConfiguration(ref confBuilder, confSub);
                }

                IConfiguration serviceConf = _subscriptions.Values.First().Service.GetTaskConfiguration(confBuilder);

                IConfiguration mergedTaskConf = Configurations.Merge(_taskInfos[id].TaskConfiguration, serviceConf);

                if (_taskInfos[id].IsActiveContextDisposed)
                {
                    LOGGER.Log(Level.Warning, string.Format("Task submit for {0} with a non-active context: spawning a new evaluator", id + 1));

                    if (_taskInfos[id].TaskStatus == TaskStatus.Failed)
                    {
                        _queuedTasks.Enqueue(id + 1);
                        _taskInfos[id].SetTaskStatus(TaskStatus.Queued);

                        SpawnNewEvaluator("_" + id + "_" + _taskInfos[id].NumRetry);
                    }
 
                    return;
                }

                _taskInfos[id].ActiveContext.SubmitTask(mergedTaskConf);

                if (TaskStatusUtils.IsRecoverable(_taskInfos[id].TaskStatus))
                {
                    _taskInfos[id].SetTaskStatus(TaskStatus.Recovering);
                }
                else
                {
                    _taskInfos[id].SetTaskStatus(TaskStatus.Submitted);
                }
            }
        }

        private void SpawnNewEvaluator(string id = "")
        {
            var request = _evaluatorRequestor.NewBuilder()
                .SetNumber(1)
                .SetMegabytes(_parameters.NewEvaluatorMemorySize)
                .SetCores(_parameters.NewEvaluatorNumCores)
                .SetRackName(_parameters.NewEvaluatorRackName)
                .SetEvaluatorBatchId(_parameters.NewEvaluatorBatchId + id)
                .Build();

            _evaluatorRequestor.Submit(request);
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

                    if (_taskInfos[destination] == null)
                    {
                        throw new ArgumentNullException("Task Info");
                    }
                    lock (_taskInfos[destination].Lock)
                    {
                        if (Completed() || Failed())
                        {
                            LOGGER.Log(Level.Warning, "Task submit for a completed or failed Task Set: ignoring");
                            _taskInfos[destination].DisposeTask();

                            return;
                        }
                        if (_taskInfos[destination].TaskStatus != TaskStatus.Running ||
                            _taskInfos[destination].TaskRunner == null)
                        {
                            var msg = string.Format("Cannot send message to {0}:", destination + 1);
                            msg += ": Task Status is " + _taskInfos[destination].TaskStatus;

                            if (_taskInfos[destination].TaskStatus == TaskStatus.Submitted && retry < _parameters.Retry)
                            {
                                LOGGER.Log(Level.Warning, msg + " Retry");
                                System.Threading.Tasks.Task.Run(() =>
                                {
                                    Thread.Sleep(_parameters.WaitTime);
                                    SendToTasks(new List<IElasticDriverMessage>() { returnMessage }, retry + 1);
                                });
                            }
                            else if (retry >= _parameters.Retry)
                            {
                                LOGGER.Log(Level.Warning, msg + " Aborting");
                                OnFail();
                            }
                            else
                            {
                                LOGGER.Log(Level.Warning, msg + " Ignoring");
                            }

                            return;
                        }

                        _taskInfos[destination].TaskRunner.Send(returnMessage.Serialize());
                    }
                }
            }
        }

        private void LogFinalStatistics()
        {
            var msg = string.Format("Total Failed Tasks: {0}\nTotal Failed Evaluators: {1}", _totFailedTasks, _totFailedEvaluators);
            msg += _subscriptions.Select(x => x.Value.LogFinalStatistics()).Aggregate((a, b) => a + "\n" + b);
            LOGGER.Log(Level.Info, msg);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
