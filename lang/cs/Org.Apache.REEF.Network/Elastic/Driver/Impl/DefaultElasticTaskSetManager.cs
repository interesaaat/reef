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
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public sealed class DefaultElasticTaskSetManager : 
        IElasticTaskSetManager, 
        IDefaultFailureEventResponse,
        IObserver<Alarm>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultElasticTaskSetManager));

        private bool _finalized;
        private volatile bool _disposed;
        private volatile bool _scheduled;
        private volatile bool _completed;
        private readonly ElasticTaskSetManagerParameters _parameters;

        private volatile int _contextsAdded;
        private int _tasksAdded;
        private int _tasksRunning;
        private volatile int _totFailedTasks;
        private volatile int _totFailedEvaluators;

        private readonly int _numTasks;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly string _driverId;
        private readonly Func<string, IConfiguration> _masterTaskConfiguration;
        private readonly Func<string, IConfiguration> _slaveTaskConfiguration;

        // Task info 0-indexed
        private readonly List<TaskInfo> _taskInfos;
        private readonly Dictionary<string, IElasticStage> _stages;
        private readonly ConcurrentQueue<int> _queuedTasks;
        private readonly ConcurrentQueue<ContextInfo> _queuedContexts;
        // Used both for knowing which evaluator the task set is responsible for and to 
        // maintain a mapping betwween evaluators and contextes.
        // This latter is necessary because evaluators may fail between context init
        // and the time when the context is installed on the evaluator
        private readonly ConcurrentDictionary<string, ContextInfo> _evaluatorToContextIdMapping;
        private IFailureState _failureStatus;
        private volatile bool _hasProgress;

        private readonly object _statusLock;

        public DefaultElasticTaskSetManager(
            int numTasks,
            IEvaluatorRequestor evaluatorRequestor,
            string driverId,
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
            _driverId = driverId;
            _masterTaskConfiguration = masterTaskConfiguration;
            _slaveTaskConfiguration = slaveTaskConfiguration ?? masterTaskConfiguration;

            _taskInfos = new List<TaskInfo>(numTasks);
            _stages = new Dictionary<string, IElasticStage>();
            _queuedTasks = new ConcurrentQueue<int>();
            _queuedContexts = new ConcurrentQueue<ContextInfo>();
            _evaluatorToContextIdMapping = new ConcurrentDictionary<string, ContextInfo>();
            _failureStatus = new DefaultFailureState();
            _hasProgress = true;

            _statusLock = new object();

            for (int i = 0; i < numTasks; i++)
            {
                _taskInfos.Add(null);
            }

            var injector = TangFactory.GetTang().NewInjector(confs);
            Type parametersType = typeof(ElasticTaskSetManagerParameters);
            _parameters = injector.GetInstance(parametersType) as ElasticTaskSetManagerParameters;

            // Set up the timeout
            List<IElasticDriverMessage> msgs = null;
            var nextTimeouts = new List<ITimeout>();

            OnTimeout(new TasksetAlarm(0, this), ref msgs, ref nextTimeouts);
        }

        public void AddStage(IElasticStage stage)
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Cannot add stage to an already built TaskManager");
            }

            _stages.Add(stage.StageName, stage);
        }

        public bool HasMoreContextToAdd()
        {
             return _contextsAdded < _numTasks;
        }

        public bool TryGetNextTaskContextId(IAllocatedEvaluator evaluator, out string identifier)
        {
            int id;
            ContextInfo cinfo;

            if (_queuedTasks.TryDequeue(out id))
            {
                identifier = Utils.BuildContextId(StagesId, id);
                cinfo = new ContextInfo(id);
                _evaluatorToContextIdMapping.TryAdd(evaluator.Id, cinfo);
                return true;
            }

            if (_queuedContexts.TryDequeue(out cinfo))
            {
                identifier = Utils.BuildContextId(StagesId, cinfo.Id);
                _evaluatorToContextIdMapping.TryAdd(evaluator.Id, cinfo);
                return true;
            }

            id = Interlocked.Increment(ref _contextsAdded);

            if (_contextsAdded > _numTasks)
            {
                LOGGER.Log(Level.Warning, "Trying to schedule too many contexts");
                identifier = string.Empty;
                return false;
            }

            identifier = Utils.BuildContextId(StagesId, id);
            cinfo = new ContextInfo(id);
            _evaluatorToContextIdMapping.TryAdd(evaluator.Id, cinfo);

            LOGGER.Log(Level.Info, "Evaluator {0} is scheduled on node {1}", evaluator.Id, evaluator.GetEvaluatorDescriptor().NodeDescriptor.HostName);

            return true;
        }

        public string GetTaskId(IActiveContext context)
        {
            var id = Utils.GetContextNum(context);
            return Utils.BuildTaskId(StagesId, id);
        }

        public string StagesId
        {
            get
            {
                if (_finalized != true)
                {
                    throw new IllegalStateException("Task set have to be built before getting its stages");
                }

                return _stages.Keys.Aggregate((current, next) => current + "+" + next);
            }
        }

        public IEnumerable<IElasticStage> IsMasterTaskContext(IActiveContext activeContext)
        {
            return _stages.Values.Where(sub => sub.IsMasterTaskContext(activeContext));
        }

        /// <summary>
        /// Get the configuration of the codecs used for data transmission.
        /// The codecs are automatically generated from the operator pipeline.
        /// </summary>
        /// <returns>A configuration object with the codecs for data transmission</returns>
        public IConfiguration GetCodecConfiguration()
        {
            var conf = TangFactory.GetTang().NewConfigurationBuilder().Build();

            foreach(var sub in _stages.Values)
            {
                sub.RootOperator.GetCodecConfiguration(ref conf);
            }

            return conf;
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

            _hasProgress = true;
            var id = Utils.GetContextNum(activeContext) - 1;

            // We reschedule the task only if the context was active (_taskInfos[id] != null) and the task was actually scheduled at least once (_taskInfos[id].TaskStatus > TaskStatus.Init)
            if (_taskInfos[id] != null && _taskInfos[id].TaskStatus > TaskState.Init)
            {
                LOGGER.Log(Level.Info, "{0} already part of Task Set: going to directly submit it", Utils.BuildTaskId(StagesId, id + 1));

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].UpdateRuntime(activeContext, activeContext.EvaluatorId);
                }

                SubmitTask(id);
            }
            else
            {
                bool isMaster = IsMasterTaskContext(activeContext).Any();
                var taskId = Utils.BuildTaskId(StagesId, id + 1);

                LOGGER.Log(Level.Info, "Task {0} to be scheduled on {1}", taskId, activeContext.EvaluatorId);

                List<IConfiguration> partialTaskConfs = new List<IConfiguration>();

                if (isMaster)
                {
                    partialTaskConfs.Add(_masterTaskConfiguration(taskId));
                }
                else
                {
                    partialTaskConfs.Add(_slaveTaskConfiguration(taskId));
                }
                                
                AddTask(taskId, activeContext, partialTaskConfs);
            }
        }

        public bool StartSubmitTasks()
        {
            lock (_statusLock)
            {
                if (_scheduled)
                {
                    return false;
                }

                if (_stages.All(sub => sub.Value.ScheduleStage()))
                {
                    _scheduled = true;

                    LOGGER.Log(Level.Info, string.Format("Scheduling {0} tasks from Taskset {1}", _tasksAdded, StagesId));
                }
            }

            return _scheduled;
        }

        public void SubmitTasks()
        {
            for (int i = 0; i < _numTasks; i++)
            {
                if (_taskInfos[i] != null)
                {
                    SubmitTask(i);
                }
            }
        }

        public IElasticTaskSetManager Build()
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
            if (IsTaskManagedBy(task.Id))
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
                    if (!TaskStateUtils.IsRunnable(_taskInfos[id].TaskStatus))
                    {
                        LOGGER.Log(Level.Info, "Received running from task {0} which is not runnable: ignoring", task.Id);
                        _taskInfos[id].DisposeTask();

                        return;
                    }

                    if (_taskInfos[id].TaskStatus != TaskState.Running)
                    {
                        if (_taskInfos[id].TaskStatus == TaskState.Recovering)
                        {
                            foreach (var sub in _stages)
                            {
                                sub.Value.AddTask(task.Id);
                            }
                        }

                        _taskInfos[id].SetTaskStatus(TaskState.Running);
                        Interlocked.Increment(ref _tasksRunning);
                    }
                }
            }
        }

        public void OnTaskCompleted(ICompletedTask taskInfo)
        {
            if (IsTaskManagedBy(taskInfo.Id))
            {
                Interlocked.Decrement(ref _tasksRunning);
                var id = Utils.GetTaskNum(taskInfo.Id) - 1;
                _hasProgress = true;

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].SetTaskStatus(TaskState.Completed);
                }
                if (Completed())
                {
                    foreach (var info in _taskInfos.Where(info => info != null && info.TaskStatus < TaskState.Failed))
                    {
                        info.DisposeTask();
                    }
                }
            }
        }

        public void OnTaskMessage(ITaskMessage message)
        {
            if (IsTaskManagedBy(message.TaskId))
            {
                var id = Utils.GetTaskNum(message.TaskId) - 1;
                var returnMessages = new List<IElasticDriverMessage>();
                _hasProgress = true;

                foreach (var sub in _stages.Values)
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
                _completed = _stages.Select(sub => sub.Value.IsCompleted).Aggregate((com1, com2) => com1 && com2);

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

        public bool IsCompleted()
        {
            return Completed() && _tasksRunning == 0;
        }

        public void OnTaskFailure(IFailedTask task)
        {
            var failureEvents = new List<IFailureEvent>();

            OnTaskFailure(task, ref failureEvents);
        }

        public void OnTimeout(Alarm alarm, ref List<IElasticDriverMessage> msgs, ref List<ITimeout> nextTimeouts)
        {
            var isInit = msgs == null;

            // Taskset is just started, init the timeouts
            if (isInit)
            {
                _hasProgress = false;
                LOGGER.Log(Level.Info, "Timeout alarm for Taskset initialized");
                nextTimeouts.Add(new TasksetTimeout(_parameters.Timeout, this));

                foreach (var sub in _stages.Values)
                {
                    sub.OnTimeout(alarm, ref msgs, ref nextTimeouts);
                }
            }
            else if (alarm.GetType() == typeof(TasksetAlarm))
            {
                if (!_hasProgress)
                {
                    if (Completed() || Failed())
                    {
                        LOGGER.Log(Level.Warning, "Taskset made no progress in the last {0}ms. Forcing Disposal.", _parameters.Timeout);
                        Dispose();
                    }
                    else
                    {
                        LOGGER.Log(Level.Error, "Taskset made no progress in the last {0}ms. Aborting.", _parameters.Timeout);
                        OnFail();
                        return;
                    }
                }
                else
                {
                    _hasProgress = false;
                    nextTimeouts.Add(new TasksetTimeout(_parameters.Timeout, this));
                }
            }
            else
            {
                foreach (var sub in _stages.Values)
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
            var msgs = new List<IElasticDriverMessage>();
            var nextTimeouts = new List<ITimeout>();

            OnTimeout(value, ref msgs, ref nextTimeouts);
        }

        public void OnTaskFailure(IFailedTask info, ref List<IFailureEvent> failureEvents)
        {
            if (IsTaskManagedBy(info.Id))
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
                        _taskInfos[id].SetTaskStatus(TaskState.Failed);
                    }

                    _taskInfos[id].Dispose();

                    return;
                }

                failureEvents = failureEvents ?? new List<IFailureEvent>();

                lock (_taskInfos[id].Lock)
                {
                    if (_taskInfos[id].TaskStatus < TaskState.Failed)
                    {
                        _taskInfos[id].SetTaskStatus(TaskState.Failed);
                    }

                    foreach (IElasticStage sub in _taskInfos[id].Stages)
                    {
                        sub.OnTaskFailure(info, ref failureEvents);
                    }

                    // Failures have to be propagated up to the service
                    _taskInfos[id].Stages.First().Service.OnTaskFailure(info, ref failureEvents);
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
                _evaluatorToContextIdMapping.TryRemove(evaluator.Id, out ContextInfo cinfo);
            }
            else
            {
                _hasProgress = true;

                if (!Completed() && !Failed())
                {
                    if (_evaluatorToContextIdMapping.TryRemove(evaluator.Id, out ContextInfo cinfo))
                    {
                        int id = cinfo.Id - 1;

                        if (_taskInfos[id] != null)
                        {
                            lock (_taskInfos[id].Lock)
                            {
                                _taskInfos[id].DropRuntime();
                                _taskInfos[id].SetTaskStatus(TaskState.Failed);
                            }
                        }

                        cinfo.NumRetry++;

                        if(cinfo.NumRetry > _parameters.NumEvaluatorFailures)
                        {
                            LOGGER.Log(Level.Error, $"Context {cinfo.Id} failed more than {_parameters.NumEvaluatorFailures} times: Aborting");
                            OnFail();
                        }

                        _queuedContexts.Enqueue(cinfo);
                    }
                    SpawnNewEvaluator(cinfo.Id);
                }
            }
        }

        public void EventDispatcher(ref IFailureEvent @event)
        {
            var id = Utils.GetTaskNum(@event.TaskId) - 1;

            _taskInfos[id].Stages.First().Service.EventDispatcher(ref @event);

            foreach (IElasticStage sub in _taskInfos[id].Stages)
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

        private void AddTask(string taskId, IActiveContext activeContext, List<IConfiguration> partialTaskConfigs)
        {
            Interlocked.Increment(ref _tasksAdded);
            var subList = new List<IElasticStage>();
            var id = Utils.GetTaskNum(taskId) - 1;

            foreach (var sub in _stages)
            {
                if (sub.Value.AddTask(taskId))
                {
                    subList.Add(sub.Value);
                    var partitionConf = sub.Value.GetPartitionConf(taskId);

                    if (partitionConf.IsPresent())
                    {
                        partialTaskConfigs.Add(partitionConf.Value);
                    }
                }
                else
                {
                    LOGGER.Log(Level.Warning, taskId + " cannot be added to stage " + sub.Key);
                    activeContext.Dispose();
                    return;
                }
            }

            var aggregatedConfs = partialTaskConfigs.Aggregate((x, y) => Configurations.Merge(x, y));

            _taskInfos[id] = new TaskInfo(aggregatedConfs, activeContext, activeContext.EvaluatorId, TaskState.Init, subList);

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
                // Check that the task was not already submitted. This may happen for instance if _scheduled is set to true
                // and a new active context message is received.
                if (_taskInfos[id].TaskStatus == TaskState.Submitted)
                {
                    return;
                }

                var subs = _taskInfos[id].Stages;
                ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();
                var rescheduleConfs = _taskInfos[id].RescheduleConfigurations;

                foreach (var sub in subs)
                {
                    ICsConfigurationBuilder confSubBuilder = TangFactory.GetTang().NewConfigurationBuilder();
                    var confSub = sub.GetTaskConfiguration(ref confSubBuilder, id + 1);

                    if (rescheduleConfs.TryGetValue(sub.StageName, out var confs))
                    {
                        foreach (var additionalConf in confs)
                        {
                            confSub = Configurations.Merge(confSub, additionalConf);
                        }
                    }

                    _stages.Values.First().Service.SerializeStageConfiguration(ref confBuilder, confSub);
                }

                IConfiguration baseConf = confBuilder
                .BindNamedParameter<ElasticServiceConfigurationOptions.DriverId, string>(
                    GenericType<ElasticServiceConfigurationOptions.DriverId>.Class,
                    _driverId)
                .Build();

                IConfiguration mergedTaskConf = Configurations.Merge(_taskInfos[id].TaskConfiguration, baseConf);

                if (_taskInfos[id].IsActiveContextDisposed)
                {
                    LOGGER.Log(Level.Warning, string.Format("Task submit for {0} with a non-active context: spawning a new evaluator", id + 1));

                    if (_taskInfos[id].TaskStatus == TaskState.Failed)
                    {
                        _queuedTasks.Enqueue(id + 1);
                        _taskInfos[id].SetTaskStatus(TaskState.Queued);

                        SpawnNewEvaluator(id);
                    }
 
                    return;
                }

                _taskInfos[id].ActiveContext.SubmitTask(mergedTaskConf);

                if (TaskStateUtils.IsRecoverable(_taskInfos[id].TaskStatus))
                {
                    _taskInfos[id].SetTaskStatus(TaskState.Recovering);
                }
                else
                {
                    _taskInfos[id].SetTaskStatus(TaskState.Submitted);
                }
            }
        }

        private void SpawnNewEvaluator(int id)
        {
            LOGGER.Log(Level.Warning, $"Spawning new evaluator for id {id}");

            var request = _evaluatorRequestor.NewBuilder()
                .SetNumber(1)
                .SetMegabytes(_parameters.NewEvaluatorMemorySize)
                .SetCores(_parameters.NewEvaluatorNumCores)
                .SetRackName(_parameters.NewEvaluatorRackName)
                .Build();

            _evaluatorRequestor.Submit(request);
        }

        public bool IsTaskManagedBy(string id)
        {
            return Utils.GetTaskStages(id) == StagesId;
        }

        public bool IsContextManagedBy(string id)
        {
            return Utils.GetContextStages(id) == StagesId;
        }

        public bool IsEvaluatorManagedBy(string id)
        {
            return _evaluatorToContextIdMapping.ContainsKey(id);
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
                        if (_taskInfos[destination].TaskStatus != TaskState.Running ||
                            _taskInfos[destination].TaskRunner == null)
                        {
                            var msg = string.Format("Cannot send message to {0}:", destination + 1);
                            msg += ": Task Status is " + _taskInfos[destination].TaskStatus;

                            if (_taskInfos[destination].TaskStatus == TaskState.Submitted && retry < _parameters.Retry)
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

                            continue;
                        }

                        _taskInfos[destination].TaskRunner.Send(returnMessage.Serialize());
                    }
                }
            }
        }

        private void LogFinalStatistics()
        {
            var msg = string.Format("Total Failed Tasks: {0}\nTotal Failed Evaluators: {1}", _totFailedTasks, _totFailedEvaluators);
            msg += _stages.Select(x => x.Value.LogFinalStatistics()).Aggregate((a, b) => a + "\n" + b);
            LOGGER.Log(Level.Info, msg);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    /// <summary>
    /// Definition of the the different states in which a task can be.
    /// </summary>
    internal enum TaskState
    {
        Init = 1,

        Queued = 2,

        Submitted = 3,

        Recovering = 4,

        Running = 5,

        Failed = 6,

        Completed = 7
    }

    /// <summary>
    /// Utility class used to recognize particular task states.
    /// </summary>
    internal static class TaskStateUtils
    {
        private static List<TaskState> recoverable = new List<TaskState>() { TaskState.Failed, TaskState.Queued };

        private static List<TaskState> notRunnable = new List<TaskState>() { TaskState.Failed, TaskState.Completed };

        /// <summary>
        /// Whether a task is recoverable or not.
        /// </summary>
        /// <param name="state">The current state of the task</param>
        /// <returns>True if the task is recoverable</returns>
        internal static bool IsRecoverable(TaskState state)
        {
            return recoverable.Contains(state);
        }

        /// <summary>
        /// Whether a task can be run or not.
        /// </summary>
        /// <param name="state">The current state of the task</param>
        /// <returns>True if the task can be run</returns>
        internal static bool IsRunnable(TaskState state)
        {
            return !notRunnable.Contains(state);
        }
    }
}
