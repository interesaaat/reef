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
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;

namespace Org.Apache.REEF.Network.Elastic.Clients.Impl
{
    /// <summary>
    /// Used by Tasks to fetch Subscription clients.
    /// </summary>
    public sealed class DefaultTaskSetService : IElasticTaskSetService
    {
        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;

        private readonly INetworkService<GeneralGroupCommunicationMessage> _networkService;

        /// <summary>
        /// Shows if the object has been disposed.
        /// </summary>
        private int _disposed;

        /// <summary>
        /// Creates a new DefaultTaskSetService and registers the task ID with the Name Server.
        /// </summary>
        /// <param name="groupConfigs">The set of serialized subscrptions configurations</param>
        /// <param name="taskId">The identifier for this task</param>
        /// <param name="networkService">The writable network service used to send messages</param>
        /// <param name="configSerializer">Used to deserialize service configuration</param>
        /// <param name="injector">injector forked from the injector that creates this instance</param>
        [Inject]
        public DefaultTaskSetService(
            [Parameter(typeof(ElasticServiceConfigurationOptions.SerializedSubscriptionConfigs))] ISet<string> subscriptionConfigs,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            StreamingNetworkService<GeneralGroupCommunicationMessage> networkService,
            AvroConfigurationSerializer configSerializer,
            IInjector injector)
        {
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();
            _networkService = networkService;

            foreach (string serializedGroupConfig in subscriptionConfigs)
            {
                IConfiguration subscriptionConfig = configSerializer.FromString(serializedGroupConfig);
                IInjector groupInjector = injector.ForkInjector(subscriptionConfig);
                var subscriptionClient = groupInjector.GetInstance<IElasticTaskSetSubscription>();
                _subscriptions[subscriptionClient.SubscriptionName] = subscriptionClient;
            }

            networkService.Register(new StringIdentifier(taskId));
        }

        /// <summary>
        /// This is to ensure all the nodes in the groups are registered before starting communications.
        /// </summary>
        /// <param name="cancellationSource"></param>
        public void Initialize(CancellationTokenSource cancellationSource = null)
        {
            try
            {
                foreach (var subscription in _subscriptions.Values)
                {
                    subscription.WaitingForRegistration(cancellationSource);
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// Gets the subscription client object for the given subscription name.
        /// </summary>
        /// <param name="subscriptionpName">The name of the subscription</param>
        /// <returns>The subscription client object</returns>
        public IElasticTaskSetSubscription GetSubscription(string subscriptionpName)
        {
            if (string.IsNullOrEmpty(subscriptionpName))
            {
                throw new ArgumentNullException("subscriptionpName");
            }
            if (!_subscriptions.ContainsKey(subscriptionpName))
            {
                throw new ArgumentException("No subscription with name: " + subscriptionpName);
            }

            return _subscriptions[subscriptionpName];
        }

        /// <summary>
        /// Disposes of the services.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _networkService.Unregister();
                _networkService.Dispose();
            }
        }
    }
}