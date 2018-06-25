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

    public class IterateBroadcastGatherSummaSlaveTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticTaskSetService _serviceClient;
        private readonly IElasticTaskSetSubscription _subscriptionClient;

        [Inject]
        public IterateBroadcastGatherSummaSlaveTask(
            IElasticTaskSetService serviceClient)
        {
            _serviceClient = serviceClient;

            _subscriptionClient = _serviceClient.GetSubscription("IterateGatherSumma");

            System.Threading.Thread.Sleep(20000);
        }

        public byte[] Call(byte[] memento)
        {
            _serviceClient.WaitForTaskRegistration();

            //// Define some initial settings

            //// The Summa breaking has num_row*num_col submatrices
            int num_row = 3;
            int num_col = 2;
            int mat_size = 12000;

            int send_size = mat_size / num_row;
            int receive_size = mat_size/ num_col;
            var V = Vector<float>.Build;
            var M = Matrix<float>.Build;

            //// Define the variables that need to be updated during each iteration

            Vector<float> v_receive = V.Dense(receive_size);
            Vector<float> v_send = V.Dense(send_size);

            //// Define the data matrices

            Matrix<float> data = M.Random(send_size, receive_size);

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

                                //// The matrix vector multiplication part

                                received = receiver.Receive();

                                v_receive = V.Dense(received);

                                //// Console.WriteLine("Slave has received the vector {0}", v_receive);

                                v_send = data.Multiply(v_receive);

                                //// Console.WriteLine("Slave has done the matrix computation and obtained {0}", v_send);

                                break;

                            case Constants.Gather:
                                var sender = workflow.Current as IElasticGather<float>;

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

                                //// Console.WriteLine("Slave has sent {0} in iteration {1}", v_send, workflow.Iteration);

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
