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

using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;
using Org.Apache.REEF.Driver.Task;
using System;
using Org.Apache.REEF.Tang.Exceptions;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Wraps all the info required to proper manager a task life cycle.
    /// </summary>
    internal sealed class TaskInfo : IDisposable
    {
        internal TaskInfo(IConfiguration config, IActiveContext context, string evaluatorId, TaskStatus status, IList<IElasticTaskSetSubscription> subscriptions)
        {
            TaskConfiguration = config;
            ActiveContext = context;
            EvaluatorId = evaluatorId;
            Subscriptions = subscriptions;
            NumRetry = 0;
            TaskStatus = status;
            RescheduleConfigurations = new Dictionary<string, IList<IConfiguration>>();
        }

        internal IConfiguration TaskConfiguration { get; private set; }

        internal IActiveContext ActiveContext { get; private set; }

        internal string EvaluatorId { get; private set; }

        internal IList<IElasticTaskSetSubscription> Subscriptions { get; private set; }

        internal Dictionary<string, IList<IConfiguration>> RescheduleConfigurations { get; set; }

        internal IRunningTask TaskRunner { get; set; }

        internal TaskStatus TaskStatus { get; set; }

        internal int NumRetry { get; set; }

        internal bool IsDisposed { get; private set; }

        internal void UpdateRuntime(IActiveContext newActiveContext, string evaluatorId)
        {
            if (ActiveContext != null)
            {
                throw new IllegalStateException("Updating Task with not null active context");
            }

            ActiveContext = newActiveContext;
            EvaluatorId = evaluatorId;
        }

        internal void DropRuntime()
        {
            ActiveContext = null;
            EvaluatorId = string.Empty;
        }

        public void Dispose()
        {
            if (!IsDisposed)
            {
                if (TaskRunner != null)
                {
                    TaskRunner.Dispose();
                    TaskRunner = null;
                }

                if (ActiveContext != null)
                {
                    ActiveContext.Dispose();
                    ActiveContext = null;
                }

                IsDisposed = true;
            }
        }
    }
}