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

namespace Org.Apache.REEF.Network.Elastic.Task
{
    internal class DefaultTaskSetSubscription : IElasticTaskSetSubscription
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DefaultTaskSetSubscription));

        [Inject]
        private DefaultTaskSetSubscription(
           [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscriptionName,
           [Parameter(typeof(GroupCommunicationConfigurationOptions.SerializedOperatorConfigs))] ISet<string> operatorConfigs,
           AvroConfigurationSerializer configSerializer,
           Workflow workflow,
           IInjector injector)
        {
            SubscriptionName = subscriptionName;
            Workflow = workflow;

            foreach (string operatorConfigStr in operatorConfigs)
            {
                IConfiguration operatorConfig = configSerializer.FromString(operatorConfigStr);

                IInjector operatorInjector = injector.ForkInjector(operatorConfig);
                string msgType = operatorInjector.GetNamedInstance<OperatorParameters.MessageType, string>(
                    GenericType<OperatorParameters.MessageType>.Class);
                int id = operatorInjector.GetNamedInstance<OperatorParameters.OperatorId, int>(
                    GenericType<OperatorParameters.OperatorId>.Class);

                Type groupCommOperatorGenericInterface = typeof(IElasticTypedOperator<>);
                Type groupCommOperatorInterface = groupCommOperatorGenericInterface.MakeGenericType(Type.GetType(msgType));
                var operatorObj = operatorInjector.GetInstance(groupCommOperatorInterface);

                Workflow.Add(operatorObj as IElasticOperator);
            }
        }

        public string SubscriptionName { get; private set; }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                Workflow.WaitForTaskRegistration(cancellationSource);
            }
            catch (OperationCanceledException e)
            {
                Logger.Log(Level.Error, "Subscription {0} failed during registration", SubscriptionName);
                throw e;
            }
        }

        public Workflow Workflow { get; private set; }

        public void Dispose()
        {
            if (Workflow != null)
            {
                Workflow.Dispose();
            }
        }
    }
}