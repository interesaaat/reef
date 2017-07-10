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
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Helper class to start Group Communication tasks.
    /// </summary>
    public interface ITaskSetManager
    {
        void AddTaskSetSubscription(IElasticTaskSetSubscription subscription);

        bool HasMoreContextToAdd { get; }

        int GetNextTaskContextId(IAllocatedEvaluator evaluator = null);

        string GetSubscriptionsId { get; }

        int GetNextTaskId(IActiveContext context = null);

        IEnumerable<IElasticTaskSetSubscription> IsMasterTaskContext(IActiveContext activeContext);

        int NumTasks { get; }

        /// <summary>
        /// Queues the task into the TaskStarter.
        /// 
        /// Once the correct number of tasks have been queued, the final Configuration
        /// will be generated and run on the given Active Context.
        /// </summary>
        /// <param name="partialTaskConfig">The partial task configuration containing Task
        /// identifier and Task class</param>
        /// <param name="activeContext">The Active Context to run the Task on</param>
        void AddTask(string taskId, IConfiguration partialTaskConfig, IActiveContext activeContext);

        bool StartSubmitTasks { get; }

        void SubmitTasks();

        void Build();

        void onTaskRunning(IRunningTask info);

        void OnTaskCompleted(ICompletedTask task);

        void OnTaskFailure(IFailedTask task);

        void OnEvaluatorFailure(IFailedEvaluator task);
    }
}
