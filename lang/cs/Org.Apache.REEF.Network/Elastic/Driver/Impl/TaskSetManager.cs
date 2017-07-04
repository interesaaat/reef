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
using Org.Apache.REEF.Network.Elastic.Driver.TaskSet;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Helper class to start Group Communication tasks.
    /// </summary>
    public sealed class TaskSetManager : ITaskSetManager
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TaskSetManager));

        private readonly object _lock;

        private int _contextsAdded;
        private int _tasksAdded;
        private readonly int _numTasks;

        private readonly List<Tuple<IConfiguration, IActiveContext>> _taskConfs;
        private readonly List<Tuple<int, TaskSetStatus>> _taskStatus;
        private readonly List<HashSet<string>> _taskToSubsMapper;

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

            _contextsAdded = 0;
            _tasksAdded = 0;
            _numTasks = numTasks;

            _taskConfs = new List<Tuple<IConfiguration, IActiveContext>>(numTasks);
            _taskStatus = new List<Tuple<int, TaskSetStatus>>(numTasks);

            _taskToSubsMapper = new List<HashSet<string>>(numTasks);

            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();

            for (int i = 0; i < numTasks; i++)
            {
                _taskConfs.Add(null);
                _taskStatus.Add(null);
                _taskToSubsMapper.Add(new HashSet<string>());
            }
        }

        public void AddTaskSetSubscription(IElasticTaskSetSubscription subscription)
        {
            _subscriptions.Add(subscription.GetSubscriptionName, subscription);
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
                return _subscriptions.Keys.Aggregate((current, next) => current + "+" + next);
            }
        }

        public int GetNextTaskId(IActiveContext context = null)
        {
            return Utils.GetContextNum(context);
        }

        public bool IsMasterTaskContext(IActiveContext activeContext)
        {
            return _subscriptions.Values.Any(sub => sub.IsMasterTaskContext(activeContext) == true);
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

            if (_taskConfs[id] != null)
            {
                throw new ArgumentException(
                    "Task Id already registered with TaskSet");
            }

            Interlocked.Increment(ref _tasksAdded);

            _taskConfs[id] = new Tuple<IConfiguration, IActiveContext>(partialTaskConfig, activeContext);

            _taskStatus[id] = new Tuple<int, TaskSetStatus>(0, TaskSetStatus.Init);

            foreach (var sub in _subscriptions)
            {
                if (sub.Value.AddTask(taskId))
                {
                    _taskToSubsMapper[id].Add(sub.Key);
                }
            }

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
                var subs = _taskToSubsMapper[i];
                ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

                foreach (var sub in subs)
                {
                    _subscriptions[sub].GetElasticTaskConfiguration(ref confBuilder);
                }

                IConfiguration subscriptionsConfiguration = confBuilder.Build();
                IConfiguration serviceConfiguration = _subscriptions.Values.First().GetService.GetElasticTaskConfiguration(subscriptionsConfiguration);
                IConfiguration mergedTaskConf = Configurations.Merge(_taskConfs[i].Item1, serviceConfiguration);
                _taskConfs[i].Item2.SubmitTask(mergedTaskConf);
            }
        }

        public void OnTaskCompleted(int taskId)
        {
            throw new NotImplementedException();
        }

        public void OnTaskFailure(int taskId)
        {
            throw new NotImplementedException();
        }
    }
}
