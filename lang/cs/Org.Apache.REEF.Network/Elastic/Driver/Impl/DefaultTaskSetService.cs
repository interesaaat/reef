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
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Task.Impl;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public sealed class DefaultTaskSetService : 
        IElasticTaskSetService,
        IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskSetService));

        private readonly string _driverId;
        private readonly int _numEvaluators;
        private readonly string _nameServerAddr;
        private readonly int _nameServerPort;
        private readonly string _defaultSubscriptionName;
        private readonly IFailureStateMachine _defaultFailureMachine;

        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;
        private readonly AvroConfigurationSerializer _configSerializer;
        private IFailureState _failureState;
        private readonly object _subsLock = new object();
        private readonly object _statusLock = new object();

        [Inject]
        private DefaultTaskSetService(
            [Parameter(typeof(ElasticServiceConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(ElasticServiceConfigurationOptions.SubscriptionName))] string defaultSubscriptionName,
            [Parameter(typeof(ElasticConfig.NumEvaluators))] int numEvaluators,
            AvroConfigurationSerializer configSerializer,
            INameServer nameServer,
            IFailureStateMachine defaultFailureStateMachine)
        {
            _driverId = driverId;
            _numEvaluators = numEvaluators;
            _defaultSubscriptionName = defaultSubscriptionName;
            _defaultFailureMachine = defaultFailureStateMachine;

            _failureState = new DefaultFailureState();
            _configSerializer = configSerializer;
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();

            IPEndPoint localEndpoint = nameServer.LocalEndpoint;
            _nameServerAddr = localEndpoint.Address.ToString();
            _nameServerPort = localEndpoint.Port;
        }

        public IElasticTaskSetSubscription DefaultTaskSetSubscription
        {
            get
            {
                lock (_subsLock)
                {
                    _subscriptions.TryGetValue(_defaultSubscriptionName, out IElasticTaskSetSubscription defaultSubscription);

                    if (defaultSubscription == null)
                    {
                        NewTaskSetSubscription(_defaultSubscriptionName, _numEvaluators, _defaultFailureMachine);
                    }
                    return _subscriptions[_defaultSubscriptionName];
                }
            }
        }

        public IElasticTaskSetSubscription NewTaskSetSubscription(
            string subscriptionName, 
            int numTasks, 
            IFailureStateMachine failureMachine = null)
        {
            if (string.IsNullOrEmpty(subscriptionName))
            {
               throw new ArgumentNullException("Subscription Name can not be null");
            }

            lock (_subsLock)
            {
                if (_subscriptions.ContainsKey(subscriptionName))
                {
                    throw new ArgumentException(
                        "Subscription Name already registered with TaskSetSubscriptionDriver");
                }

                var subscription = new DefaultTaskSetSubscription(
                    subscriptionName,
                    _configSerializer,
                    numTasks, this, failureMachine ?? _defaultFailureMachine);
                _subscriptions[subscriptionName] = subscription;

                return subscription;
            }
        }

        public void RemoveTaskSetSubscription(string subscriptionName)
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

        public string DriverId
        {
            get
            {
                return _driverId;
            }
        }

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

        public IConfiguration GetTaskConfiguration(ICsConfigurationBuilder subscriptionsConf)
        {
            var partialConf = subscriptionsConf
                .BindNamedParameter<GroupCommConfigurationOptions.DriverId, string>(
                    GenericType<GroupCommConfigurationOptions.DriverId>.Class,
                    DriverId)
                .Build();
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            confBuilder.BindSetEntry<GroupCommConfigurationOptions.SerializedGroupConfigs, string>(
                GenericType<GroupCommConfigurationOptions.SerializedGroupConfigs>.Class,
                _configSerializer.ToString(partialConf));

            return confBuilder.Build();
        }

        public IFailureState OnTaskFailure(IFailedTask value)
        {
            lock (_statusLock)
            {
                foreach (IElasticTaskSetSubscription sub in _subscriptions.Values)
                {
                    _failureState.FailureState = Math.Max(_failureState.FailureState, sub.FailureState.FailureState);
                }

                return _failureState;
            }
        }

        public void EventDispatcher(IFailureEvent @event)
        {
            switch ((DefaultFailureStateEvents)@event.FailureEvent)
            {
                case DefaultFailureStateEvents.Reconfigure:
                    OnReconfigure(@event as IReconfigure);
                    break;
                case DefaultFailureStateEvents.Reschedule:
                    OnReschedule(@event as IReschedule);
                    break;
                case DefaultFailureStateEvents.Stop:
                    OnStop(@event as IStop);
                    break;
            }
        }

        public void OnReconfigure(IReconfigure info)
        {
            LOGGER.Log(Level.Info, "Reconfiguring the service");
        }

        public void OnReschedule(IReschedule rescheduleEvent)
        {
            LOGGER.Log(Level.Info, "Going to reschedule a task");
        }

        public void OnStop(IStop stopEvent)
        {
            LOGGER.Log(Level.Info, "Going to stop the service and reschedule a task");
        }
    }
}
