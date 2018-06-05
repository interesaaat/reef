﻿// Licensed to the Apache Software Foundation (ASF) under one
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
    public class IterateBroadcastGatherMasterTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticTaskSetService _serviceClient;
        private readonly IElasticTaskSetSubscription _subscriptionClient;

        [Inject]
        public IterateBroadcastGatherMasterTask(IElasticTaskSetService serviceClient)
        {
            _serviceClient = serviceClient;

            _subscriptionClient = _serviceClient.GetSubscription("IterateGather");

            System.Threading.Thread.Sleep(20000);
        }

        public byte[] Call(byte[] memento)
        {
            _serviceClient.WaitForTaskRegistration();

            //// Define some initial settings such as the number of workers

            int num_workers = 2;
            int feature_dimension = 100;
            int receive_size = 100;
            int num_data_each = 250;
            int num_data_total = num_data_each * num_workers;
            float step_size = 0.001F;
            double loss = 10000;

            var V = Vector<float>.Build;
            var M = Matrix<float>.Build;
            
            //// Define the variables that need to be updated during each iteration

            Vector<float> wt = V.Random(feature_dimension);
            Vector<float> gradient = V.Dense(feature_dimension);
            Matrix<float> received_messages = M.Dense(receive_size * num_workers, 1);

            //// Define the data at the master node

            Matrix<float> data = M.Dense(num_data_total, feature_dimension);
            Vector<float> labels = V.Random(num_data_total);
            Vector<float> Ay = V.Dense(feature_dimension);
            Vector<float> x_true = V.Dense(feature_dimension);
            Console.WriteLine("Master has generated the data and obtained {0}", data);
            Console.WriteLine("Master has generated the labels and obtained {0}", labels);

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

                                if (workflow.Iteration.Equals(1))
                                {
                                    Console.WriteLine("No data to send at the first iteration!");

                                    System.Threading.Thread.Sleep(1000);

                                    break;

                                }

                                //// Computing the SVD of a matrix

                                Matrix<float> m = Matrix<float>.Build.Random(4, 4);
                                var svd = m.Svd();

                                //// Generating a vector and broadcast it

                                var v_message = wt.ToArray();
                                sender.Send(v_message);

                                Console.WriteLine("Master has sent the vector {0} in iteration {1}", wt, workflow.Iteration);

                                System.Threading.Thread.Sleep(1000);

                                break;

                            case Constants.Gather:

                                var receiver = workflow.Current as IElasticGather<float>;

                                if (workflow.Iteration.Equals(1))
                                {
                                    var received = receiver.Receive();

                                    data = M.Dense(feature_dimension, num_data_total, received).Transpose();

                                    Console.WriteLine("Master has received the data {0}", data);

                                    Ay = data.Transpose().Multiply(labels);

                                    x_true = data.Solve(labels);

                                    break;
                                }

                                var updates = receiver.Receive();
                                Console.WriteLine("Master has received {0} in iteration {1}", string.Join(",", updates), workflow.Iteration);

                                int num_floats = updates.Length;
                                Console.WriteLine("Master has received {0} floating point numbers in iteration {1}", num_floats, workflow.Iteration);

                                int num_responses = num_floats / feature_dimension;

                                received_messages = M.Dense(num_floats,1, updates);
                                Console.WriteLine("Master has received {0} in iteration {1} before reshaping", received_messages, workflow.Iteration);

                                received_messages = received_messages.Reshape(feature_dimension, num_responses);
                                Console.WriteLine("Master has received {0} in iteration {1} after reshaping", received_messages, workflow.Iteration);

                                //// Compute the gradient and update the result
                                gradient = received_messages.RowSums()-Ay;
                                wt = wt - step_size * gradient;

                                //loss = (data.Multiply(wt) - labels).L2Norm();
                                loss = (wt - x_true).L2Norm();

                                Console.WriteLine("The loss is {0} in iteration {1}", loss, workflow.Iteration);

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
