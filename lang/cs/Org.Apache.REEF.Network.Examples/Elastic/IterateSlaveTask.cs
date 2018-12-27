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
using System.Linq;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public class IterateSlaveTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticContext _contextClient;
        private readonly IElasticStage _stageClient;

        [Inject]
        public IterateSlaveTask(IElasticContext serviceClient)
        {
            _contextClient = serviceClient;

            _stageClient = _contextClient.GetStage("Iterate");
        }

        public byte[] Call(byte[] memento)
        {
            _contextClient.WaitForTaskRegistration();

            using (var workflow = _stageClient.Workflow)
            {
                try
                {
                    while (workflow.MoveNext())
                    {
                        switch (workflow.Current.OperatorName)
                        {
                            default:
                                Console.WriteLine("Iteration {0}", workflow.Iteration);

                                System.Threading.Thread.Sleep(1000);
                                break;
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

        static float NextFloat(Random random)
        {
            double mantissa = (random.NextDouble() * 2.0) - 1.0;
            double exponent = Math.Pow(2.0, random.Next(-126, 128));
            return (float)(mantissa * exponent);
        }
    }
}
