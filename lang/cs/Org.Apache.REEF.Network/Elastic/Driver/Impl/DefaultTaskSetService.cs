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
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Driver.Context;

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
            [Parameter(typeof(ElasticServiceConfigurationOptions.DefaultSubscriptionName))] string defaultSubscriptionName,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumEvaluators))] int numEvaluators,
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

        public IElasticTaskSetSubscription DefaultTaskSetSubscription()
        {
            lock (_subsLock)
            {
                IElasticTaskSetSubscription defaultSubscription;
                _subscriptions.TryGetValue(_defaultSubscriptionName, out defaultSubscription);

                if (defaultSubscription == null)
                {
                    NewTaskSetSubscription(_defaultSubscriptionName, _numEvaluators, _defaultFailureMachine);
                }
                return _subscriptions[_defaultSubscriptionName];
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

        public string GetContextSubscriptions(IActiveContext activeContext)
        {
            return Utils.GetContextSubscriptions(activeContext);
        }

        public IConfiguration GetServiceConfiguration()
        {
            IConfiguration serviceConfig = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, 
                    GenericType<StreamingNetworkService<GroupCommunicationMessage>>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(serviceConfig)
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
            return subscriptionsConf
                .BindNamedParameter<ElasticServiceConfigurationOptions.DriverId, string>(
                    GenericType<ElasticServiceConfigurationOptions.DriverId>.Class,
                    _driverId)
                .Build();
        }

        public void SerializeSubscriptionConfiguration(ref ICsConfigurationBuilder confBuilder, IConfiguration subscriptionConfiguration)
        {
            confBuilder.BindSetEntry<ElasticServiceConfigurationOptions.SerializedSubscriptionConfigs, string>(
                GenericType<ElasticServiceConfigurationOptions.SerializedSubscriptionConfigs>.Class,
                _configSerializer.ToString(subscriptionConfiguration));
        }

        public void SerializeOperatorConfiguration(ref ICsConfigurationBuilder confBuilder, IConfiguration operatorConfiguration)
        {
            confBuilder.BindSetEntry<GroupCommunicationConfigurationOptions.SerializedOperatorConfigs, string>(
                GenericType<GroupCommunicationConfigurationOptions.SerializedOperatorConfigs>.Class,
                _configSerializer.ToString(operatorConfiguration));
        }

        public void OnTaskFailure(IFailedTask value, ref List<IFailureEvent> failureEvents)
        {       
        }

        public void EventDispatcher(IFailureEvent @event, ref List<IElasticDriverMessage> failureResponses)
        {
            switch ((DefaultFailureStateEvents)@event.FailureEvent)
            {
                case DefaultFailureStateEvents.Reconfigure:
                    failureResponses.AddRange(OnReconfigure(@event as IReconfigure));
                    break;
                case DefaultFailureStateEvents.Reschedule:
                    failureResponses.AddRange(OnReschedule(@event as IReschedule));
                    break;
                case DefaultFailureStateEvents.Stop:
                    failureResponses.AddRange(OnStop(@event as IStop));
                    break;
                default:
                    break;
            }
        }

        public List<IElasticDriverMessage> OnReconfigure(IReconfigure info)
        {
            LOGGER.Log(Level.Info, "Reconfiguring the service");

            lock (_statusLock)
            {
                _failureState.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReconfigure));
            }

            return new List<IElasticDriverMessage>();
        }

        public List<IElasticDriverMessage> OnReschedule(IReschedule rescheduleEvent)
        {
            LOGGER.Log(Level.Info, "Going to reschedule a task");

            lock (_statusLock)
            {
                _failureState.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReschedule));
            }

            return new List<IElasticDriverMessage>();
        }

        public List<IElasticDriverMessage> OnStop(IStop stopEvent)
        {
            LOGGER.Log(Level.Info, "Going to stop the service and reschedule a task");

            lock (_statusLock)
            {
                _failureState.Merge(new DefaultFailureState((int)DefaultFailureStates.StopAndReschedule));
            }

            return new List<IElasticDriverMessage>();
        }
    }
}
