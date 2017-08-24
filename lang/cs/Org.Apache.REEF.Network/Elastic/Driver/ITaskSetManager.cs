﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Failures;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Class defining how group of tasks sharing similar scheduling semantics are managed.
    /// TaskSets subscribe to Subscriptions in order to define tasks logic.
    /// </summary>
    public interface ITaskSetManager : IFailureResponse
    {
        void AddTaskSetSubscription(IElasticTaskSetSubscription subscription);

        bool HasMoreContextToAdd();

        int GetNextTaskContextId(IAllocatedEvaluator evaluator = null);

        string SubscriptionsId { get; }

        int GetNextTaskId(IActiveContext context = null);

        void Build();

        IEnumerable<IElasticTaskSetSubscription> IsMasterTaskContext(IActiveContext activeContext);

        void AddTask(string taskId, IConfiguration partialTaskConfig, IActiveContext activeContext);

        bool StartSubmitTasks();

        void SubmitTasks();

        void OnTaskRunning(IRunningTask info);

        void OnTaskCompleted(ICompletedTask task);

        void OnTaskMessage(ITaskMessage message);

        bool Done();

        void Dispose();

        void OnEvaluatorFailure(IFailedEvaluator task);

        void OnFail();
    }
}
