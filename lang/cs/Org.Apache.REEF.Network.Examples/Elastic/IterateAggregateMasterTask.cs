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
    public class IterateAggregateMasterTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticTaskSetService _serviceClient;
        private readonly IElasticTaskSetSubscription _subscriptionClient;

        [Inject]
        public IterateAggregateMasterTask(IElasticTaskSetService serviceClient)
        {
            _serviceClient = serviceClient;

            _subscriptionClient = _serviceClient.GetSubscription("IterateAggregate");
        }

        public byte[] Call(byte[] memento)
        {
            _serviceClient.WaitForTaskRegistration();

            var rand = new Random();
            int n = 10;

            using (var workflow = _subscriptionClient.Workflow)
            {
                try
                {
                    var model = new float[n];

                    for (int i = 0; i < n; i++)
                    {
                        model[i] = NextFloat(rand);
                    }

                    var checkpointable = workflow.GetCheckpointableState();
                    checkpointable.MakeCheckpointable(model);

                    while (workflow.MoveNext())
                    {
                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.AggregationRing:
                                var aggregator = workflow.Current as IElasticAggregationRing<float[]>;

                                aggregator.Send(model);

                                Console.WriteLine("Master has sent model size {0} in iteration {1}", model.Length, workflow.Iteration);

                                var update = aggregator.Receive();

                                //// Update the model
                                for (int i = 0; i < n; i++)
                                {
                                    model[i] = ++update;
                                }

                                Console.WriteLine("Master has received model size {0} in iteration {1}", update.Length, workflow.Iteration);
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
            _subscriptionClient.Cancel();
            _serviceClient.Dispose();

            Console.WriteLine("Disposed.");
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        static float NextFloat(Random random)
        {
            double mantissa = (random.NextDouble() * 2.0) - 1.0;
            double exponent = Math.Pow(2.0, random.Next(-126, 128));
            return (float)(mantissa * exponent);
        }
    }
}
