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

using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Interface;
using System;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Used to create Communication Groups for Group Communication Operators.
    /// Also manages configuration for Group Communication tasks/services.
    /// </summary>
    public interface IElasticTaskSetService : IFailureResponse
    {
        IElasticTaskSetSubscription DefaultElasticTaskSetSubscription { get; }

        /// <summary>
        /// Create a new CommunicationGroup with the given name and number of tasks/operators. 
        /// </summary>
        /// <param name="taskSetName">The new group name</param>
        /// <returns>The new task set subscription</returns>
        IElasticTaskSetSubscription NewElasticTaskSetSubscription(string subscriptionName, int numTasks);

        /// <summary>
        /// remove a communication group
        /// Throw ArgumentException if the group does not exist
        /// </summary>
        /// <param name="groupName"></param>
        void RemoveElasticTaskSetSubscription(string subscriptionName);

        IEnumerator<IElasticTaskSetSubscription> GetSubscriptions { get; }

        /// <summary>
        /// Get the service configuration required for running Group Communication on Reef tasks.
        /// </summary>
        /// <returns>The service configuration for the Reef tasks</returns>
        IConfiguration GetServiceConfiguration();

        /// <summary>
        /// Get the configuration for a particular task.  
        ///
        /// The task may belong to many Communication Groups, so each one is serialized
        /// in the configuration as a SerializedGroupConfig.
        ///
        /// The user must merge their part of task configuration (task id, task class)
        /// with this returned Group Communication task configuration.
        /// </summary>
        /// <param name="taskId">The id of the task Configuration to generate</param>
        /// <returns>The Group Communication task configuration with communication group and
        /// operator configuration set.</returns>
        IConfiguration GetElasticTaskConfiguration(ICsConfigurationBuilder subscriptionsConf);

        string GetDriverId { get; }
    }
}
