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
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Driver.TaskSet;
using Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl;
using System.Threading;
using Org.Apache.REEF.Driver.Context;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Used to configure Group Communication operators in Reef driver.
    /// All operators in the same Communication Group run on the the 
    /// same set of tasks.
    /// </summary>
    public sealed class ElasticTaskSetSubscription : FailureResponse, IElasticTaskSetSubscription
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticTaskSetSubscription));

        private readonly string _subscriptionName;
        private int _tasksAdded;
        private int _contextsAdded;
        private int _numTasks;
        private int _numContexts;
        private bool _finalized;
        private readonly AvroConfigurationSerializer _confSerializer;
        private readonly IElasticTaskSetService _elasticService;

        private readonly Dictionary<string, TaskSetStatus> _taskSet;
        private TaskSetStatus _status;
        private readonly IElasticTaskSetSubscription _prev;
        private readonly Dictionary<string, IElasticTaskSetSubscription> _next;
        private ElasticOperator _root;
        private readonly object _taskSetLock = new object();
        private readonly object _statusLock = new object();

        /// <summary>
        /// Create a new CommunicationGroupDriver.
        /// </summary>
        /// <param name="groupName">The communication group name</param>
        /// <param name="driverId">Identifier of the Reef driver</param>
        /// <param name="numTasks">The number of tasks each operator will use</param>
        /// <param name="fanOut"></param>
        /// <param name="confSerializer">Used to serialize task configuration</param>
        public ElasticTaskSetSubscription(
            string subscriptionName,
            AvroConfigurationSerializer confSerializer,
            int numTasks,
            IElasticTaskSetSubscription prev = null,
            IElasticTaskSetService elasticService = null)
        {
            _confSerializer = confSerializer;
            _subscriptionName = subscriptionName;
            _tasksAdded = 0;
            _numTasks = numTasks;
            _numContexts = numTasks;
            _contextsAdded = 0;
            _finalized = false;
            _status = TaskSetStatus.Init;
            _prev = prev;
            _root = new ElasticEmpty(this);
            _elasticService = elasticService;

            _taskSet = new Dictionary<string, TaskSetStatus>();
            _next = new Dictionary<string, IElasticTaskSetSubscription>();
        }

        public IElasticTaskSetSubscription NewElasticTaskSetSubscription(string subscriptiontName, IElasticTaskSetSubscription prev, int numTasks = -1)
        {
            if (_next.ContainsKey(subscriptiontName))
            {
                throw new ArgumentException(
                       "Subscription Name already present");
            }

            var next = new ElasticTaskSetSubscription(subscriptiontName, _confSerializer, numTasks < 0 ? _numTasks : numTasks, this);
            _next[subscriptiontName] = next;

            return next;
        }

        public string GetSubscriptionName
        {
            get
            {
                return _subscriptionName;
            }
        }

        public void AddTask(string taskId)
        {
            if (!_finalized)
            {
                throw new IllegalStateException(
                    "CommunicationGroupDriver must call Build() before adding tasks to the group.");
            }

            lock (_taskSetLock)
            {
                if (_taskSet.ContainsKey(taskId))
                {
                    throw new ArgumentException(
                       "Task Id already registered with TaskSet");
                }

                _tasksAdded++;
                _taskSet[taskId] = TaskSetStatus.Init;
            }

            GetRootOperator.AddTask(taskId);
        }

        public int GetTaskContextId()
        {
            if (!_finalized || DoneWithContexts)
            {
                throw new IllegalStateException(
                    "CommunicationGroupDriver must call Build() before adding tasks to the group.");
            }

            return Interlocked.Increment(ref _contextsAdded);
        }

        public int GetTaskId
        {
            get
            {
                if (!_finalized)
                {
                    throw new IllegalStateException(
                        "CommunicationGroupDriver must call Build() before adding tasks to the group.");
                }

                return _tasksAdded;
            }
        }

        public bool DoneWithContexts
        {
            get
            {
                return _contextsAdded == _numContexts && _status == TaskSetStatus.Init;
            }
        }

        public bool DoneWithTasks
        {
            get
            {
                return _tasksAdded == _numTasks && _status == TaskSetStatus.Init;
            }
        }

        public bool IsMasterTaskContext(IActiveContext activeContext)
        {
            int id = Utils.GetContextNum(activeContext);
            return id == 0;
        }

        /// <summary>
        /// Get the Task Configuration for this communication group. 
        /// Must be called only after all tasks have been added to the CommunicationGroupDriver.
        /// </summary>
        /// <param name="taskId">The task id of the task that belongs to this Communication Group</param>
        /// <returns>The Task Configuration for this communication group</returns>
        public IConfiguration GetElasticTaskConfiguration(string taskId)
        {
            if (!_taskSet.ContainsKey(taskId))
            {
                return null;
            }

            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<GroupCommConfigurationOptions.DriverId, string>(
                    GenericType<GroupCommConfigurationOptions.DriverId>.Class,
                    GetElasticService.GetDriverId)
                .BindNamedParameter<GroupCommConfigurationOptions.CommunicationGroupName, string>(
                    GenericType<GroupCommConfigurationOptions.CommunicationGroupName>.Class,
                    _subscriptionName);

            GetRootOperator.GetElasticTaskConfiguration(out confBuilder);

            return confBuilder.Build();
        }

        public IElasticTaskSetService GetElasticService
        {
            get
            {
                if (_elasticService == null)
                {
                    if (_prev == null)
                    {
                        throw new IllegalStateException("No Elastic Service was set");
                    }

                    return _prev.GetElasticService;
                }

                return _elasticService;
            }
        }

        public ElasticOperator GetRootOperator
        {
            get
            {
                return _root;
            }
        }

       public IElasticTaskSetSubscription Build()
       {
            if (_finalized == true)
            {
                throw new IllegalStateException("Subscription cannot be built more than once");
            }

            _finalized = true;

            foreach (var next in _next.Values)
            {
                next.Build();
            }
            return this;
        }

        public override void OnNext(IFailedEvaluator value)
        {
            lock (_statusLock)
            {
                if (_status == TaskSetStatus.Running)
                {
                    _status = TaskSetStatus.Running;
                }
            }
        }

        public override void OnNext(IFailedTask value)
        {
            lock (_statusLock)
            {
                _status = TaskSetStatus.Running;
            }
        }

        public override void OnStopAndResubmit()
        {
            throw new NotImplementedException();
        }

        public override void OnStopAndRecompute()
        {
            throw new NotImplementedException();
        }
    }
}