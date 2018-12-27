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

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public class IterateBroadcast2MasterTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticContext _contextClient;
        private readonly IElasticStage _stageClient;

        [Inject]
        public IterateBroadcast2MasterTask(IElasticContext serviceClient)
        {
            _contextClient = serviceClient;

            _stageClient = _contextClient.GetStage("IterateBroadcast");

            System.Threading.Thread.Sleep(20000);
        }

        public byte[] Call(byte[] memento)
        {
            _contextClient.WaitForTaskRegistration();

            var rand = new Random();
            int number = 0;

            System.Threading.Thread.Sleep(20000);

            using (var workflow = _stageClient.Workflow)
            {
                try
                {
                    while (workflow.MoveNext())
                    {
                        number = rand.Next();

                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.Broadcast:
                                var sender = workflow.Current as IElasticBroadcast<byte[]>;

                                sender.Send(new byte[] { 1 });

                                System.Threading.Thread.Sleep(100);

                                Console.WriteLine("Master has sent {0} in first workflow iteration {1}", number, workflow.Iteration);
                                break;
                            default:
                                throw new InvalidOperationException("Operation " + workflow.Current + " in first workflow not implemented");
                        }
                    }

                    while (workflow.MoveNext())
                    {
                        number = rand.Next();

                        switch (workflow.Current.OperatorName)
                        {
                            case Constants.Broadcast:
                                var sender = workflow.Current as IElasticBroadcast<byte[]>;

                                sender.Send(new byte[] { 1 });

                                System.Threading.Thread.Sleep(100);

                                Console.WriteLine("Master has sent {0} in second workflow iteration {1}", number, workflow.Iteration);
                                break;
                            default:
                                throw new InvalidOperationException("Operation " + workflow.Current + " in second workflow not implemented");
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
            _stageClient.Cancel();
        }

        public void Dispose()
        {
            _stageClient.Cancel();
            _contextClient.Dispose();
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
