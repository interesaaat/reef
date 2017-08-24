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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Task;
using System.Threading;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public class BroadcastMasterTask : ITask
    {
        private readonly IElasticTaskSetService _serviceClient;
        private readonly IElasticTaskSetSubscription _subscriptionClient;

        private readonly CancellationTokenSource _cancellationSource;

        [Inject]
        public BroadcastMasterTask(
            IElasticTaskSetService serviceClient)
        {
            _serviceClient = serviceClient;
            _cancellationSource = new CancellationTokenSource();

            _subscriptionClient = _serviceClient.GetSubscription("Broadcast");
        }

        public byte[] Call(byte[] memento)
        {
            _serviceClient.WaitForTaskRegistration(_cancellationSource);

            var rand = new Random();
            int number = 0;

            using (var workflow = _subscriptionClient.Workflow)
            {
                try
                {
                    while (workflow.MoveNext())
                    {
                        number = rand.Next();

                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.Broadcast:
                                var sender = workflow.Current as IElasticBroadcast<int>;

                                sender.Send(number, _cancellationSource);

                                Console.WriteLine("Master has sent {0}", number);
                                break;
                            default:
                                throw new InvalidOperationException("Operation " + workflow.Current + " in workflow not implemented");
                        }
                    }
                }
                catch (Exception e)
                {
                    workflow.Throw(e);
                }
            }

            return null;
        }

        public void Handle(IDriverMessage message)
        {
        }

            public void Dispose()
        {
            _cancellationSource.Cancel();
            _serviceClient.Dispose();

            Console.WriteLine("Disposed.");
        }
    }
}
