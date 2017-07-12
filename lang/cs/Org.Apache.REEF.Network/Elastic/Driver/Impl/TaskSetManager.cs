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
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Helper class to start Group Communication tasks.
    /// </summary>
    public sealed class TaskSetManager : ITaskSetManager
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TaskSetManager));

        private readonly object _lock;
        private bool _finalized;

        private int _contextsAdded;
        private int _tasksAdded;
        private readonly int _numTasks;

        private readonly List<TaskInfo> _taskInfos; 
        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;

        /// <summary>
        /// Create new TaskStarter.
        /// After adding the correct number of tasks to the TaskStarter, the
        /// Tasks will be started on their given active context.
        /// </summary>
        /// <param name="groupCommDriver">The IGroupCommuDriver for the Group Communication tasks</param>
        /// <param name="numTasks">The number of Tasks that need to be added before
        /// the Tasks will be started. </param>
        public TaskSetManager(int numTasks)
        {
            _lock = new object();
            _finalized = false;

            _contextsAdded = 0;
            _tasksAdded = 0;
            _numTasks = numTasks;

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

            _subscriptions.Add(subscription.GetSubscriptionName, subscription);
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

        public string GetSubscriptionsId
        {
            get
            {
                if (_finalized == true)
                {
                    throw new IllegalStateException("Subscription cannot be built more than once");
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
            return _subscriptions.Values
                .Where(sub => sub.IsMasterTaskContext(activeContext));
        }

        public int NumTasks
        {
            get
            {
                return _numTasks;
            }
        }

        /// <summary>
        /// Queues the task into the TaskStarter.
        /// 
        /// Once the correct number of tasks have been queued, the final Configuration
        /// will be generated and run on the given Active Context.
        /// </summary>
        /// <param name="partialTaskConfig">The partial task configuration containing Task
        /// identifier and Task class</param>
        /// <param name="activeContext">The Active Context to run the Task on</param>
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
                return _tasksAdded == _numTasks;
            }
        }

        /// <summary>
        /// Starts the Master Task followed by the Slave Tasks.
        /// </summary>
        public void SubmitTasks()
        {
            for (int i = 0; i < _numTasks; i++)
            {
                var subs = _taskInfos[i].Subscriptions;
                ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

                foreach (var sub in subs)
                {
                    sub.GetElasticTaskConfiguration(ref confBuilder);
                }

                IConfiguration serviceConfiguration = _subscriptions.Values.First().GetService.GetElasticTaskConfiguration(confBuilder);
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

        public void onTaskRunning(IRunningTask task)
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

        public void OnTaskCompleted(ICompletedTask taskId)
        {
            throw new NotImplementedException();
        }

        public void OnTaskFailure(IFailedTask task)
        {
            if (BelongsTo(task.Id))
            {
                var id = Utils.GetTaskNum(task.Id) - 1;

                lock (_lock)
                {
                    if (_taskInfos[id].TaskStatus < TaskStatus.Failed)
                    {
                        _taskInfos[id].TaskStatus = TaskStatus.Failed;
                    }
                }

                if (task.AsError() is OperatorException)
                {
                    foreach (IElasticTaskSetSubscription sub in _taskInfos[id].Subscriptions)
                    {
                        sub.OnTaskFailure(task);
                    }

                    // _service.GenerateActionOnFailure

                    // Some mechanism implementing the action
                }
            }
        }

        public void OnEvaluatorFailure(IFailedEvaluator evaluator)
        {
            throw new NotImplementedException();
        }

        private bool BelongsTo(string id)
        {
            return Utils.GetTaskSubscriptions(id) == GetSubscriptionsId;
        }
    }
}
