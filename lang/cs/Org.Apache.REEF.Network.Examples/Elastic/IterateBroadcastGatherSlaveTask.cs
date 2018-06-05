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
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Operators;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Extension;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    using System.Linq;
    using static MatrixExtensionMethods;

    public class IterateBroadcastGatherSlaveTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticTaskSetService _serviceClient;
        private readonly IElasticTaskSetSubscription _subscriptionClient;

        [Inject]
        public IterateBroadcastGatherSlaveTask(
            IElasticTaskSetService serviceClient)
        {
            _serviceClient = serviceClient;

            _subscriptionClient = _serviceClient.GetSubscription("IterateGather");

            System.Threading.Thread.Sleep(20000);
        }

        public byte[] Call(byte[] memento)
        {
            _serviceClient.WaitForTaskRegistration();

            //// Define some initial settings

            int node_Id = 1;
            int num_workers = 2;
            int receive_size = 100;
            int send_size = 100;
            int feature_dimension = 100;
            int num_data = 250;
            var V = Vector<float>.Build;
            var M = Matrix<float>.Build;

            //// Define the variables that need to be updated during each iteration

            Vector<float> v_receive = V.Dense(receive_size);
            Vector<float> v_send = V.Dense(send_size);

            //// Define the data matrices

            Matrix<float> data = M.Random(num_data, feature_dimension);

            Console.WriteLine("Slave has generated the data and obtained {0}", data);

            //// These are some original settings

            var rand = new Random();

            float number = new Random().Next(100);

            float[] received;

            using (var workflow = _subscriptionClient.Workflow)
            {
                try
                {
                    while (workflow.MoveNext())
                    {
                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.Broadcast:

                                var receiver = workflow.Current as IElasticBroadcast<float[]>;

                                if (workflow.Iteration.Equals(1))
                                {
                                    Console.WriteLine("No receive in the first iteration!");

                                    break;
                                }

                                //// The matrix vector multiplication part

                                received = receiver.Receive();

                                v_receive = V.Dense(received);

                                Console.WriteLine("Slave has received the vector {0}", v_receive);

                                v_send = data.Transpose().Multiply(data.Multiply(v_receive));

                                Console.WriteLine("Slave has done the gradient computation and obtained {0}", v_send);

                                break;

                            case Constants.Gather:
                                var sender = workflow.Current as IElasticGather<float>;

                                if (workflow.Iteration.Equals(1))
                                {

                                    //// In the first iteration, the worker node sends the data to the master node
                                    //// TODO: For now I just send the data set. However, it should be the case that the system does the thing for you
                                    //// Ideally, what should be the case is that the data are already stored at the workers

                                    float[] data_slave = data.Transpose().Enumerate().ToArray();

                                    sender.Send(data_slave);

                                    Console.WriteLine("The slave sends its data, this is not ideal and should be changed!");

                                    Console.WriteLine("The data are {0}", data);

                                    break;

                                }

                                /*
                                if (rand.Next(100) < 1)
                                {
                                    Console.WriteLine("I am going to die. Bye. before");

                                    if (rand.Next(100) < 100)
                                    {
                                        throw new Exception("Die. before");
                                    }
                                    else
                                    {
                                        Environment.Exit(0);
                                    }
                                }
                                */

                                //// sender.Send(new float[] { number,number,number });
                                sender.Send(v_send.ToArray());

                                Console.WriteLine("Slave has sent {0} in iteration {1}", v_send, workflow.Iteration);

                                /*
                                if (rand.Next(100) < 1)
                                {
                                    Console.WriteLine("I am going to die. Bye. after");

                                    if (rand.Next(100) < 100)
                                    {
                                        throw new Exception("Die. After");
                                    }
                                    else
                                    {
                                        Environment.Exit(0);
                                    }
                                }
                                */

                                break;
                            default:
                                throw new InvalidOperationException(string.Format("Operation {0} in workflow not implemented", workflow.Current.OperatorName));
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

        private static void Reshape(Matrix<double> m, int size)
        {
            return;
        }

        public void Handle(IDriverMessage message)
        {
        }

        public void Dispose()
        {
            _subscriptionClient.Cancel();
            _serviceClient.Dispose();
        }

        public void OnNext(ICloseEvent value)
        {
            _subscriptionClient.Cancel();
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
