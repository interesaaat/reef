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

using System.Collections.Generic;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Interface;
using System;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Used to configure Group Communication operators in Reef driver.
    /// All operators in the same Communication Group run on the the 
    /// same set of tasks.
    /// </summary>
    public interface IElasticTaskSetSubscriptionDriver : ITaskSetStatusView, IObserver<IFailedEvaluator>, IObserver<IFailedTask>
    {
        /// <summary>
        /// Returns the list of task ids that belong to this Communication Group
        /// </summary>
        IList<string> TaskSet { get; }

        IElasticTaskSetSubscriptionDriver Next { get; }

        IElasticTaskSetSubscriptionDriver Prev { get; }

        /// <summary>
        /// Create a new CommunicationGroup with the given name and number of tasks/operators. 
        /// </summary>
        /// <param name="taskSetName">The new group name</param>
        /// <returns>The new task set subscription</returns>
        IElasticTaskSetSubscriptionDriver NewTaskSetSubscription(string taskSetName);

        /// <summary>
        /// Create a new CommunicationGroup with the given name and number of tasks/operators. 
        /// </summary>
        /// <returns>The new task set subscription</returns>
        IElasticOperator getRootOperator();

        /// <summary>
        /// Finalizes the CommunicationGroupDriver.
        /// After the CommunicationGroupDriver has been finalized, no more operators may
        /// be added to the group.
        /// </summary>
        /// <returns>The same finalized CommunicationGroupDriver</returns>
        IElasticTaskSetSubscriptionDriver Build();

        /// <summary>
        /// Add a task to the communication group.
        /// The CommunicationGroupDriver must have called Build() before adding tasks to the group.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        void AddTask(string taskId);
  
        /// <summary>
        /// Get the Task Configuration for this communication group. 
        /// Must be called only after all tasks have been added to the CommunicationGroupDriver.
        /// </summary>
        /// <param name="taskId">The task id of the task that belongs to this Communication Group</param>
        /// <returns>The Task Configuration for this communication group</returns>
        IConfiguration GetGroupTaskConfiguration(string taskId);
    }
}