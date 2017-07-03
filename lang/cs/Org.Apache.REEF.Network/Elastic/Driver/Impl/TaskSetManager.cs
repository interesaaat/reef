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
using Org.Apache.REEF.Network.Utilities;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Driver.TaskSet;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Helper class to start Group Communication tasks.
    /// </summary>
    public sealed class TaskSetManager : ITaskSetManager
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TaskSetManager));

        private readonly object _lock;

        private readonly List<Tuple<IConfiguration, IActiveContext>> _taskConfs;
        private readonly List<Tuple<int, TaskSetStatus>> _taskStatus; 

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

            _taskConfs = new List<Tuple<IConfiguration, IActiveContext>>(numTasks);
            _taskStatus = new List<Tuple<int, TaskSetStatus>>(numTasks);

            for (int i = 0; i < numTasks; i++)
            {
                _taskConfs.Add(null);
                _taskStatus.Add(null);
            }
        }

        public int GetNextTaskId(IActiveContext context = null)
        {
            return Utils.GetContextNum(context);
        }

        public int NumTasks
        {
            get
            {
                return _taskStatus.Capacity;
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
        public void AddTask(int taskId, IConfiguration partialTaskConfig, IActiveContext activeContext)
        {
            if (_taskConfs[taskId] != null)
            {
                throw new ArgumentException(
                    "Task Id already registered with TaskSet");
            }

            lock (_lock)
            {
                _taskConfs[taskId] = new Tuple<IConfiguration, IActiveContext>(partialTaskConfig, activeContext);

                _taskStatus[taskId] = new Tuple<int, TaskSetStatus>(0, TaskSetStatus.Init);
            }
        }

        public bool InitComplete(int numAddedTasks)
        {
            return numAddedTasks == _taskStatus.Capacity;
        }

        /// <summary>
        /// Starts the Master Task followed by the Slave Tasks.
        /// </summary>
        public void StartTasks(IConfiguration subscriptionTaskConfiguration)
        {
            try
            {
                _taskConfs.Single(tuple => Utils.GetIsMasterTask(tuple.Item1));
            }
            catch (InvalidOperationException)
            {
                LOGGER.Log(Level.Error, "There must be exactly one master task. The driver has been misconfigured.");
                throw;
            }

            foreach (Tuple<IConfiguration, IActiveContext> confTuple in _taskConfs)
            {
                IConfiguration mergedTaskConf = Configurations.Merge(confTuple.Item1, subscriptionTaskConfiguration);
                confTuple.Item2.SubmitTask(mergedTaskConf);
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
