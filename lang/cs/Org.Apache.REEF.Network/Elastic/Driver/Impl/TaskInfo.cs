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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;
using Org.Apache.REEF.Driver.Task;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    internal sealed class TaskInfo
    {
        private readonly IConfiguration _configuration;
        private readonly IActiveContext _activeContext;
        private int _numRetry;
        private TaskStatus _status;
        private readonly IList<IElasticTaskSetSubscription> _subscriptions;
        private IRunningTask _taskRunner;

        /// <summary>
        /// Construct a TaskInfo that wraps task state, task configuration, and active context for submitting the task 
        /// </summary>
        /// <param name="taskState"></param>
        /// <param name="config"></param>
        /// <param name="context"></param>
        internal TaskInfo(IConfiguration config, IActiveContext context, TaskStatus status, IList<IElasticTaskSetSubscription> subscriptions)
        {
            _configuration = config;
            _activeContext = context;
            _numRetry = 0;
            _status = status;
            _subscriptions = subscriptions;
        }

        internal IConfiguration TaskConfiguration
        {
            get { return _configuration; }
        }

        internal IActiveContext ActiveContext
        {
            get { return _activeContext; }
        }

        internal IList<IElasticTaskSetSubscription> Subscriptions
        {
            get { return _subscriptions; }
        }

        internal IRunningTask TaskRunner
        {
            get
            {
                return _taskRunner;
            }
            set
            {
                _taskRunner = value;
            }
        }

        internal TaskStatus TaskStatus
        {
            get
            {
                return _status;
            }
            set
            {
                _status = value;
            }
        }

        internal int NumRetry
        {
            get
            {
                return _numRetry;
            }

            set
            {
                _numRetry = value;
            }
        }
    }
}