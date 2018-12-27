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
    using static MatrixExtensionMethods;

    public class IterateBroadcastGatherSlaveTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticContext _contextClient;
        private readonly IElasticStage _stageClient;

        [Inject]
        public IterateBroadcastGatherSlaveTask(
            IElasticContext serviceClient)
        {
            _contextClient = serviceClient;

            _stageClient = _contextClient.GetStage("IterateGather");

            System.Threading.Thread.Sleep(20000);
        }

        public byte[] Call(byte[] memento)
        {
            _contextClient.WaitForTaskRegistration();

            var rand = new Random();

            var number = rand.Next();
            var numbers = new int[100];
            for (int i = 0; i < 100; i++)
            {
                numbers[i] = rand.Next();
            }

            using (var workflow = _stageClient.Workflow)
            {
                try
                {
                    while (workflow.MoveNext())
                    {
                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.Broadcast:
                                var receiver = workflow.Current as IElasticBroadcast<byte[]>;

                                receiver.Receive();

                                Console.WriteLine("Slave has received in iteration {0}", workflow.Iteration);
                                break;

                            case Constants.Gather:
                                var sender = workflow.Current as IElasticGather<byte>;

                                if (rand.Next(100) < 0)
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

                                sender.Send( new byte[1] { 1 });

                                Console.WriteLine("Slave has sent {0} in iteration {1}", number, workflow.Iteration);

                                if (rand.Next(100) < 0)
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
            _stageClient.Cancel();
            _contextClient.Dispose();
        }

        public void OnNext(ICloseEvent value)
        {
            _stageClient.Cancel();
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
