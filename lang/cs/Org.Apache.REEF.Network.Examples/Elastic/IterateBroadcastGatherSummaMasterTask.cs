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
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Extension;
using System.Linq;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public class IterateBroadcastGatherSummaMasterTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticTaskSetService _serviceClient;
        private readonly IElasticTaskSetSubscription _subscriptionClient;

        [Inject]
        public IterateBroadcastGatherSummaMasterTask(IElasticTaskSetService serviceClient)
        {
            _serviceClient = serviceClient;

            _subscriptionClient = _serviceClient.GetSubscription("IterateGatherSumma");

            System.Threading.Thread.Sleep(20000);
        }

        public byte[] Call(byte[] memento)
        {
            _serviceClient.WaitForTaskRegistration();

            //// Define some initial settings such as the number of workers

            int num_workers = 2;

            int num_row = 3;
            int num_col = 2;
            int mat_size = 12000;

            int receive_size = mat_size / num_row;
            int send_size = mat_size / num_col;

            var V = Vector<float>.Build;
            var M = Matrix<float>.Build;
            

            //// Define the variables

            Matrix<float> received_messages = M.Dense(receive_size * num_workers, 1);
            Vector<float> wt = V.Random(send_size);

            using (var workflow = _subscriptionClient.Workflow)
            {
                try
                {
                    while (workflow.MoveNext())
                    {
                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.Broadcast:
                                var sender = workflow.Current as IElasticBroadcast<float[]>;

                                //// Generating a vector and broadcast it

                                var v_message = wt.ToArray();
                                sender.Send(v_message);

                                //// Console.WriteLine("Master has sent the vector {0} in iteration {1}", wt, workflow.Iteration);

                                System.Threading.Thread.Sleep(1000);

                                break;

                            case Constants.Gather:

                                var receiver = workflow.Current as IElasticGather<float>;

                                var updates = receiver.Receive();
                                //// Console.WriteLine("Master has received {0} in iteration {1}", string.Join(",", updates), workflow.Iteration);

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

        public void OnNext(ICloseEvent value)
        {
            _subscriptionClient.Cancel();
        }

        public void Dispose()
        {
            _subscriptionClient.Cancel();
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
