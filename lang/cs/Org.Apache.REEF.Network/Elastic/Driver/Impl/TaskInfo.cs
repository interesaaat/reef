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

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Wraps all the info required to proper manager a task life cicle.
    /// </summary>
    internal sealed class TaskInfo
    {
        internal TaskInfo(IConfiguration config, IActiveContext context, TaskStatus status, IList<IElasticTaskSetSubscription> subscriptions)
        {
            TaskConfiguration = config;
            ActiveContext = context;
            Subscriptions = subscriptions;
            NumRetry = 0;
            TaskStatus = status;
        }

        internal IConfiguration TaskConfiguration { get; private set; }

        internal IActiveContext ActiveContext { get; private set; }

        internal IList<IElasticTaskSetSubscription> Subscriptions { get; private set; }

        internal IRunningTask TaskRunner { get; set; }

        internal TaskStatus TaskStatus { get; set; }

        internal int NumRetry { get; set; }
    }
}