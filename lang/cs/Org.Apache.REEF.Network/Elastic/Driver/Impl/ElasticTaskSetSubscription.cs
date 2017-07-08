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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
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
        private bool _finalized;
        private readonly AvroConfigurationSerializer _confSerializer;
        private readonly IElasticTaskSetService _elasticService;
        private readonly int _numTasks;
        private int _tasksAdded;

        private readonly TaskSetManager _taskSet;
        ////private TaskSetStatus _status;
        private ElasticOperator _root;
        private int _numOperators;

        private readonly object _statusLock = new object();
        private readonly object _tasksLock = new object();

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
            IElasticTaskSetService elasticService)
        {
            _confSerializer = confSerializer;
            _subscriptionName = subscriptionName;
            _finalized = false;
            _numTasks = numTasks;
            _tasksAdded = 0;
            _root = new Empty(this);
            _elasticService = elasticService;

            _taskSet = new TaskSetManager(numTasks);
            _tasksLock = new TaskSetManager(numTasks);
        }

        public string GetSubscriptionName
        {
            get
            {
                return _subscriptionName;
            }
        }

        public int GetNextOperatorId()
        {
            return Interlocked.Increment(ref _numOperators);
        }

        public bool AddTask(string taskId)
        {
            if (!_finalized)
            {
                throw new IllegalStateException(
                    "CommunicationGroupDriver must call Build() before adding tasks to the group.");
            }

            lock (_tasksLock)
            {
                if (_tasksAdded >= _numTasks)
                {
                    return false;
                }

                _tasksAdded++;
            }

            GetRootOperator.AddTask(taskId);

            return true;
        }

        public bool IsMasterTaskContext(IActiveContext activeContext)
        {
            if (!_finalized)
            {
                throw new IllegalStateException(
                    "CommunicationGroupDriver must call Build() before adding tasks to the group.");
            }

            int id = Utils.GetContextNum(activeContext);
            return id == 1;
        }

        public IElasticTaskSetService GetService
        {
            get
            {
                return _elasticService;
            }
        }

        /// <summary>
        /// Get the Task Configuration for this communication group. 
        /// Must be called only after all tasks have been added to the CommunicationGroupDriver.
        /// </summary>
        /// <param name="taskId">The task id of the task that belongs to this Communication Group</param>
        /// <returns>The Task Configuration for this communication group</returns>
        public void GetElasticTaskConfiguration(ref ICsConfigurationBuilder builder)
        {
            builder = builder
                .BindSetEntry<GroupCommConfigurationOptions.CommunicationGroupNames, string>(
                    GenericType<GroupCommConfigurationOptions.CommunicationGroupNames>.Class,
                    _subscriptionName);

                GetRootOperator.GetElasticTaskConfiguration(ref builder);
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

            return this;
        }

        public override void OnNext(IFailedTask value)
        {
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