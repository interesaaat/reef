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
using System.Threading;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    internal class DefaultTaskSetSubscription : IElasticTaskSetSubscription
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DefaultTaskSetSubscription));

        private readonly CancellationSource _cancellationSource;

        private readonly object _lock;
        private bool _disposed;

        [Inject]
        private DefaultTaskSetSubscription(
           [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscriptionName,
           [Parameter(typeof(GroupCommunicationConfigurationOptions.SerializedOperatorConfigs))] IList<string> operatorConfigs,
           [Parameter(typeof(StartIteration))] int startIteration,
           AvroConfigurationSerializer configSerializer,
           Workflow workflow,
           CommunicationLayer commLayer,
           CancellationSource cancellationSource,
           IInjector injector)
        {
            SubscriptionName = subscriptionName;
            Workflow = workflow;

            _cancellationSource = cancellationSource;
            _disposed = false;
            _lock = new object();

            Workflow.CancellationSource = _cancellationSource;

            System.Threading.Thread.Sleep(20000);

            foreach (string operatorConfigStr in operatorConfigs)
            {
                IConfiguration operatorConfig = configSerializer.FromString(operatorConfigStr);

                IInjector operatorInjector = injector.ForkInjector(operatorConfig);
                string msgType = operatorInjector.GetNamedInstance<MessageType, string>(
                    GenericType<MessageType>.Class);

                Type groupCommOperatorGenericInterface = typeof(IElasticTypedOperator<>);
                Type groupCommOperatorInterface = groupCommOperatorGenericInterface.MakeGenericType(Type.GetType(msgType));
                var operatorObj = operatorInjector.GetInstance(groupCommOperatorInterface);

                Workflow.Add(operatorObj as IElasticOperator);
            }
        }

        public string SubscriptionName { get; private set; }

        public Workflow Workflow { get; private set; }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource = null)
        {
            try
            {
                Workflow.WaitForTaskRegistration(cancellationSource ?? _cancellationSource.Source);
            }
            catch (OperationCanceledException e)
            {
                Logger.Log(Level.Error, "Subscription {0} failed during registration", SubscriptionName);
                throw e;
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (!_disposed)
                {
                    if (Workflow != null)
                    {
                        Workflow.Dispose();
                    }

                    _disposed = true;
                }
            }  
        }

        public void Cancel()
        {
            if (!_cancellationSource.IsCancelled())
            {
                _cancellationSource.Cancel();

                Logger.Log(Level.Info, "Received request to close Subscription", SubscriptionName);
            }
        }
    }
}