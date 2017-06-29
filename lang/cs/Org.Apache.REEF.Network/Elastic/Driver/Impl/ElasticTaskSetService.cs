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
using System.Globalization;
using System.Net;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Elastic.Driver.TaskSet;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Used to create Communication Groups for Group Communication Operators on the Reef driver.
    /// Also manages configuration for Group Communication tasks/services.
    /// </summary>
    public sealed class ElasticTaskSetService : FailureResponse, IElasticTaskSetService
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ElasticTaskSetService));

        private readonly string _driverId;
        private readonly string _nameServerAddr;
        private readonly int _nameServerPort;
        private int _contextIds;
        private readonly string _defaultSubscriptionName;

        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;
        private TaskSetStatus _status;
        private readonly AvroConfigurationSerializer _configSerializer;
        private readonly object _subsLock = new object();
        private readonly object _statusLock = new object();

        /// <summary>
        /// Create a new GroupCommunicationDriver object.
        /// </summary>
        /// <param name="driverId">Identifier for the REEF driver</param>
        /// <param name="defaultSubscriptionName">default communication group name</param>
        /// <param name="nameServer">Used to map names to ip addresses</param>
        [Inject]
        private ElasticTaskSetService(
            [Parameter(typeof(GroupCommConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(GroupCommConfigurationOptions.GroupName))] string defaultSubscriptionName,
            AvroConfigurationSerializer configSerializer,
            INameServer nameServer)
        {
            _driverId = driverId;
            _contextIds = -1;
            _defaultSubscriptionName = defaultSubscriptionName;
            _status = TaskSetStatus.WAITING;

            _configSerializer = configSerializer;
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();

            IPEndPoint localEndpoint = nameServer.LocalEndpoint;
            _nameServerAddr = localEndpoint.Address.ToString();
            _nameServerPort = localEndpoint.Port;
        }
 
        public IElasticTaskSetSubscription DefaultElasticTaskSetSubscription
        {
            get
            {
                lock (_subsLock)
                {
                    _subscriptions.TryGetValue(_defaultSubscriptionName, out IElasticTaskSetSubscription defaultSubscription);

                    if (defaultSubscription == null)
                    {
                        NewElasticTaskSetSubscription(_defaultSubscriptionName);
                    }
                    return _subscriptions[_defaultSubscriptionName];
                }
            }
        }

        /// <summary>
        /// Create a new CommunicationGroup with the given name and number of tasks/operators. 
        /// </summary>
        /// <param name="groupName">The new group name</param>
        /// <param name="numTasks">The number of tasks/operators in the group.</param>
        /// <returns>The new Communication Group</returns>
        public IElasticTaskSetSubscription NewElasticTaskSetSubscription(string subscriptionName)
        {
            if (string.IsNullOrEmpty(subscriptionName))
            {
               throw new ArgumentNullException("subscriptionName");
            }

            lock (_subsLock)
            {
                if (_subscriptions.ContainsKey(subscriptionName))
                {
                    throw new ArgumentException(
                        "Subscription Name already registered with TaskSetSubscriptionDriver");
                }

                var subscription = new ElasticTaskSetSubscription(
                    subscriptionName,
                    _configSerializer);
                _subscriptions[subscriptionName] = subscription;
                return subscription;
            }
        }

        /// <summary>
        /// Remove a group from the GroupCommDriver
        /// Throw ArgumentException if the group does not exist
        /// </summary>
        /// <param name="groupName"></param>
        public void RemoveElasticTaskSetSubscription(string subscriptionName)
        {
            lock (_subsLock)
            {
                if (!_subscriptions.ContainsKey(subscriptionName))
                {
                    throw new ArgumentException(
                        "Subscription Name is not registered with TaskSetSubscriptionDriver");
                }
 
                _subscriptions.Remove(subscriptionName);
            }
        }

        /// <summary>
        /// Generates context configuration with a unique identifier.
        /// </summary>
        /// <returns>The configured context configuration</returns>
        public IConfiguration GetContextConfiguration()
        {
            int contextNum = Interlocked.Increment(ref _contextIds);
            string id = GetTaskContextName(contextNum);

            return ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, id)
                .Build();
        }

        /// <summary>
        /// Get the service configuration required for running Group Communication on Reef tasks.
        /// </summary>
        /// <returns>The service configuration for the Reef tasks</returns>
        public IConfiguration GetServiceConfiguration()
        {
            IConfiguration serviceConfig = ServiceConfiguration.ConfigurationModule
                .Set(
                    ServiceConfiguration.Services, 
                    GenericType<StreamingNetworkService<GeneralGroupCommunicationMessage>>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(serviceConfig)
                .BindImplementation(
                    GenericType<IObserver<IRemoteMessage<NsMessage<GeneralGroupCommunicationMessage>>>>.Class,
                    GenericType<GroupCommNetworkObserver>.Class)
                .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                    GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                    _nameServerAddr)
                .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                    GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                    _nameServerPort.ToString(CultureInfo.InvariantCulture))
                .BindImplementation(GenericType<INameClient>.Class,
                    GenericType<NameClient>.Class)
                .Build();
        }

        /// <summary>
        /// Get the configuration for a particular task.  
        /// The task may belong to many Communication Groups, so each one is serialized
        /// in the configuration as a SerializedGroupConfig.
        /// The user must merge their part of task configuration (task id, task class)
        /// with this returned Group Communication task configuration.
        /// </summary>
        /// <param name="taskId">The id of the task Configuration to generate</param>
        /// <returns>The Group Communication task configuration with communication group and
        /// operator configuration set.</returns>
        public IConfiguration GetElasticTaskConfiguration(string taskId)
        {
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            foreach (IElasticTaskSetSubscription sub in _subscriptions.Values)
            {
                var taskConf = sub.GetElasticTaskConfiguration(taskId);
                if (taskConf != null)
                {
                    confBuilder.BindSetEntry<GroupCommConfigurationOptions.SerializedGroupConfigs, string>(
                        GenericType<GroupCommConfigurationOptions.SerializedGroupConfigs>.Class,
                        _configSerializer.ToString(taskConf));
                }
            }

            return confBuilder.Build();
        }

        /// <summary>
        /// Gets the context number associated with the Active Context id.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>The context number associated with the active context id</returns>
        public int GetContextNum(IActiveContext activeContext)
        {
            string[] parts = activeContext.Id.Split('-');
            if (parts.Length != 2)
            {
               throw new ArgumentException("Invalid id in active context");
            }

            return int.Parse(parts[1], CultureInfo.InvariantCulture);
        }

        public string GetDriverId
        {
            get
            {
                return _driverId;
            }
        }

        private string GetTaskContextName(int contextNum)
        {
            return string.Format(CultureInfo.InvariantCulture, "TaskContext-{0}", contextNum);
        }

        public new void OnNext(IFailedEvaluator value)
        {
            lock (_statusLock)
            {
                if (_status == TaskSetStatus.RUNNING)
                {
                    _status = TaskSetStatus.RUNNING;
                }
            }
        }

        public new void OnNext(IFailedTask value)
        {
            lock (_statusLock)
            {
                _status = TaskSetStatus.RUNNING;
            }
        }
    }
}
