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

using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Used to create Subscriptions for fault tolerant Task Sets.
    /// Also manages configurations for Group Communication operators/services.
    /// </summary>
    [DefaultImplementation(typeof(DefaultTaskSetService))]
    public interface IElasticTaskSetService : IFailureResponse
    {
        /// <summary>
        /// Creates a Subscription with the default settings. 
        /// The subscription lifecicle is managed by the service.
        /// </summary>
        /// <returns>A new Task Set Subscription with default parameters</returns>
        IElasticTaskSetSubscription DefaultTaskSetSubscription();

        /// <summary>
        /// Creates a new Task Set Subscription.
        ///  The subscription lifecicle is managed by the service.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription</param>
        /// <param name="numTasks">The number of tasks required by the subscription</param>
        /// <param name="failureMachine">An optional failure machine governing the subscription</param>
        /// <returns>The new Task Set Subscrption</returns>
        IElasticTaskSetSubscription NewTaskSetSubscription(string subscriptionName, int numTasks, IFailureStateMachine failureMachine = null);

        /// <summary>
        /// Remove a Task Set Subscription from the service.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription</param>
        void RemoveTaskSetSubscription(string subscriptionName);

        /// <summary>
        /// Get the subscriptions from the context.
        /// </summary>
        /// <param name="activeContext">An activeContext</param>
        /// <returns>The Subscription of the context</returns>
        string GetContextSubscriptions(IActiveContext activeContext);

        /// <summary>
        /// Generate the service configuration object.
        /// This method is used to properly configure the Context with the service.
        /// </summary>
        /// <returns>The Service Configuration</returns>
        IConfiguration GetServiceConfiguration();

        /// <summary>
        /// At task submission time the following steps are executed:
        /// 1) Each subscription the task is registered to generates a task subscription
        /// 2) Internally each configuration generated by subscriptions contains a configuration entry for each
        /// operator defining the subscription. Such operator configurations are serialized using 
        /// {@link Org.Apache.REEF.Network.Elastic.Driver.IElasticTaskSetService#SerializeOperatorConfiguration}
        /// 3) Tasks subscriptions are serialized into a configuration
        /// 4) The service Task configuration is added to the configuration object containing the serialized subscription confs
        /// 5) the Task configuration is merged with the configuraiton object of 4) to generate the final task configuration
        /// </summary>
        ///
        /// <summary>
        /// Creates a generic Task Configuration object for the tasks registering to the service.
        /// </summary>
        /// <param name="subscriptionsConf">The configuration of the subscription the task will register to</param>
        /// <returns>The configuration for the Task with added service parameters</returns>
        IConfiguration GetTaskConfiguration(ICsConfigurationBuilder subscriptionsConf);

        /// <summary>
        /// Appends a subscription configuration to a configuration builder object.
        /// </summary>
        /// <param name="confBuilder">The configuration where the subscription configuration will be appended to</param>
        /// <param name="subscriptionConf">The subscription configuration at hand</param>
        /// <returns>The configuration containing the serialized subscription configuration</returns>
        void SerializeSubscriptionConfiguration(ref ICsConfigurationBuilder confBuilder, IConfiguration subscriptionConf);

        /// <summary>
        /// Append an operator configuration to a configuration builder object.
        /// </summary>
        /// <param name="confBuilder">The configuration where the operator configuration will be appended to</param>
        /// <param name="subscriptionConf">The operator configuration at hand</param>
        /// <returns>The configuration containing the serialized operator configuration</returns>
        void SerializeOperatorConfiguration(ref ICsConfigurationBuilder confBuilder, IConfiguration operatorConf);
    }
}
