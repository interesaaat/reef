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
using System.Reflection;
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
using System.Collections;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Used to configure Group Communication operators in Reef driver.
    /// All operators in the same Communication Group run on the the 
    /// same set of tasks.
    /// </summary>
    public sealed class ElasticTaskSetSubscriptionDriver : IElasticTaskSetSubscriptionDriver
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticTaskSetSubscriptionDriver));

        private readonly string _subscriptionName;
        private readonly string _driverId;
        private int _tasksAdded;
        private bool _finalized;
        private readonly AvroConfigurationSerializer _confSerializer;

        private readonly Dictionary<string, TaskSetStatus> _taskSet;
        private TaskSetStatus _status;
        private readonly IElasticTaskSetSubscriptionDriver _prev;
        private readonly Dictionary<string, IElasticTaskSetSubscriptionDriver> _next;
        private readonly object _taskSetLock;
        private readonly object _statusLock;

        /// <summary>
        /// Create a new CommunicationGroupDriver.
        /// </summary>
        /// <param name="groupName">The communication group name</param>
        /// <param name="driverId">Identifier of the Reef driver</param>
        /// <param name="numTasks">The number of tasks each operator will use</param>
        /// <param name="fanOut"></param>
        /// <param name="confSerializer">Used to serialize task configuration</param>
        public ElasticTaskSetSubscriptionDriver(
            string subscriptionName,
            AvroConfigurationSerializer confSerializer,
            IElasticTaskSetSubscriptionDriver prev = null)
        {
            _confSerializer = confSerializer;
            _subscriptionName = subscriptionName;
            _tasksAdded = 0;
            _finalized = false;
            _status = TaskSetStatus.WAITING;
            _prev = prev;

            _taskSet = new Dictionary<string, TaskSetStatus>();
            _next = new Dictionary<string, IElasticTaskSetSubscriptionDriver>();
        }

        public IElasticTaskSetSubscriptionDriver NewElasticTaskSetSubscription(string subscriptiontName, IElasticTaskSetSubscriptionDriver prev)
        {
            var next = new ElasticTaskSetSubscriptionDriver(subscriptiontName, _confSerializer, this);
            return next;
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
                if(_taskSet.ContainsKey(taskId))
                {
                    throw new ArgumentException(
                       "Task Id already registered with TaskSet");
                }

                _tasksAdded++;
                _taskSet[taskId] = TaskSetStatus.WAITING;
            }

            getRootOperator().AddTask(taskId);
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
                    _driverId)
                .BindNamedParameter<GroupCommConfigurationOptions.CommunicationGroupName, string>(
                    GenericType<GroupCommConfigurationOptions.CommunicationGroupName>.Class,
                    _subscriptionName);

            getRootOperator().GetElasticTaskConfiguration(out confBuilder);

            return confBuilder.Build();
        }

        public IElasticOperator getRootOperator()
        {
            throw new NotImplementedException();
        }

        IElasticTaskSetSubscriptionDriver IElasticTaskSetSubscriptionDriver.Build()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public void OnNext(IFailedEvaluator value)
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnNext(IFailedTask value)
        {
            throw new NotImplementedException();
        }
    }
}