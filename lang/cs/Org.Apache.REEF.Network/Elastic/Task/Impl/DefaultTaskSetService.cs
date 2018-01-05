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
using Org.Apache.REEF.Network.Elastic.Comm.Impl;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Used by Tasks to fetch Subscriptions.
    /// </summary>
    internal sealed class DefaultTaskSetService : IElasticTaskSetService
    {
        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;
        private readonly string _taskId;

        private readonly INetworkService<GroupCommunicationMessage> _networkService;

        private readonly object _lock;
        private bool _disposed;

        /// <summary>
        /// Creates a new DefaultTaskSetService and registers the task ID with the Name Server.
        /// </summary>
        /// <param name="subscriptionConfigs">The set of serialized subscriptions configurations</param>
        /// <param name="taskId">The identifier for this task</param>
        /// <param name="networkService">The writable network service used to send messages</param>
        /// <param name="configSerializer">Used to deserialize service configuration</param>
        /// <param name="injector">injector forked from the injector that creates this instance</param>
        [Inject]
        public DefaultTaskSetService(
            [Parameter(typeof(ElasticServiceConfigurationOptions.SerializedSubscriptionConfigs))] ISet<string> subscriptionConfigs,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            StreamingNetworkService<GroupCommunicationMessage> networkService,
            AvroConfigurationSerializer configSerializer,
            TaskToDriverMessageDispatcher taskToDriverDispatcher, // Otherwise the correct instance does not propagate through
            DriverMessageHandler ringDriverSource,
            CheckpointService checkpointService,
            IInjector injector)
        {
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();
            _networkService = networkService;
            _taskId = taskId;

            _disposed = false;
            _lock = new object();

            foreach (string serializedGroupConfig in subscriptionConfigs)
            {
                IConfiguration subscriptionConfig = configSerializer.FromString(serializedGroupConfig);
                IInjector subInjector = injector.ForkInjector(subscriptionConfig);

                var subscriptionClient = subInjector.GetInstance<IElasticTaskSetSubscription>();

                _subscriptions[subscriptionClient.SubscriptionName] = subscriptionClient;
            }

            _networkService.Register(new StringIdentifier(_taskId));
        }

        public CancellationTokenSource CancellationSource { get; set; }

        /// <summary>
        /// This is to ensure all the nodes in the groups are registered before starting communications.
        /// </summary>
        /// <param name="cancellationSource"></param>
        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource = null)
        {
            foreach (var subscription in _subscriptions.Values)
            {
                subscription.WaitForTaskRegistration(cancellationSource);
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
            lock (_lock)
            {
                if (!_disposed)
                {
                    foreach (var sub in _subscriptions.Values)
                    {
                        sub.Dispose();
                    }

                    _networkService.Unregister();

                    _disposed = true;
                }
            }
        }
    }
}