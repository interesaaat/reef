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
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Operators;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public class BroadcastReduceSlaveTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticContext _contextClient;
        private readonly IElasticStage _stageClient;

        [Inject]
        public BroadcastReduceSlaveTask(
            IElasticContext serviceClient)
        {
            _contextClient = serviceClient;

            _stageClient = _contextClient.GetStage("IterateBroadcastReduce");
        }

        public byte[] Call(byte[] memento)
        {
            _contextClient.WaitForTaskRegistration();

            var received = 0;
            var rand = new Random();

            using (var workflow = _stageClient.Workflow)
            {
                try
                {
                    while (workflow.MoveNext())
                    {
                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.Broadcast:
                                var receiver = workflow.Current as IElasticBroadcast<int>;

                                if (rand.Next(100) < 5)
                                {
                                    Console.WriteLine("I am going to die. Bye. before receive");

                                    throw new Exception("Die. before receive");
                                }

                                received = receiver.Receive();

                                if (rand.Next(100) < 5)
                                {
                                    Console.WriteLine("I am going to die. Bye. after receive");

                                    throw new Exception("Die. after receive");
                                }

                                Console.WriteLine("Slave has received {0} in iteration {1}", received, workflow.Iteration);
                                break;

                            case Constants.Reduce:
                                var sender = workflow.Current as IElasticReduce<int>;

                                if (rand.Next(100) < 5)
                                {
                                    Console.WriteLine("I am going to die. Bye. before send");

                                    throw new Exception("Die. before send");
                                }

                                sender.Send(received);

                                if (rand.Next(100) < 5)
                                {
                                    Console.WriteLine("I am going to die. Bye. after send");

                                    throw new Exception("Die. after send");
                                }

                                Console.WriteLine("Slave has sent {0} in iteration {1}", received, workflow.Iteration);
                                break;
                            default:
                                throw new InvalidOperationException("Operation {0} in workflow not implemented");
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
