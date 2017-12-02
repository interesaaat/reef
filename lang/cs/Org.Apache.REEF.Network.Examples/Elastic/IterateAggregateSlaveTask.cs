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
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Common.Tasks.Events;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public class IterateAggregateSlaveTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticTaskSetService _serviceClient;
        private readonly IElasticTaskSetSubscription _subscriptionClient;

        [Inject]
        public IterateAggregateSlaveTask([Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            IElasticTaskSetService serviceClient)
        {
            _serviceClient = serviceClient;

            _subscriptionClient = _serviceClient.GetSubscription("IterateAggregate");
        }

        public byte[] Call(byte[] memento)
        {
            _serviceClient.WaitForTaskRegistration();

            var rand = new Random();

            using (var workflow = _subscriptionClient.Workflow)
            {
                try
                {
                    var checkpointable = workflow.GetCheckpointableState();

                    while (workflow.MoveNext())
                    {
                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.AggregationRing:
                                var aggregator = workflow.Current as IElasticAggregationRing<int[]>;

                                if (rand.Next(100) < 5)
                                {
                                    Console.WriteLine("I am going to die. Bye. before");

                                    if (rand.Next(100) < 50)
                                    {
                                        throw new Exception("Die. before");
                                    }
                                    else
                                    {
                                        Environment.Exit(0);
                                    }
                                }
                                    var rec = aggregator.Receive();

                                Console.WriteLine("Slave has received {0} in iteration {1}", string.Join(",", rec), workflow.Iteration);

                                // Update the model, die in case
                                for (int i = 0; i < rec.Length; i++)
                                {
                                    rec[i] += 0;
                                }

                                if (rand.Next(100) < 5)
                                {
                                    Console.WriteLine("I am going to die. Bye. mid");

                                    if (rand.Next(100) < 50)
                                    {
                                        throw new Exception("Die. mid");
                                    }
                                    else
                                    {
                                        Environment.Exit(0);
                                    }
                                }

                                    aggregator.Send(rec);

                                Console.WriteLine("Slave has sent {0} in iteration {1}", string.Join(",", rec), workflow.Iteration);

                                if (rand.Next(100) < 5)
                                {
                                    Console.WriteLine("I am going to die. Bye.");

                                    if (rand.Next(100) < 50)
                                    {
                                        throw new Exception("Die. after");
                                    }
                                    else
                                    {
                                        Environment.Exit(0);
                                    }
                                }
                                    break;
                            default:
                                throw new InvalidOperationException("Operation " + workflow.Current + " not implemented");
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

        public void OnNext(ICloseEvent value)
        {
            _subscriptionClient.Cancel();
        }

        public void Dispose()
        {
            _serviceClient.Dispose();
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
