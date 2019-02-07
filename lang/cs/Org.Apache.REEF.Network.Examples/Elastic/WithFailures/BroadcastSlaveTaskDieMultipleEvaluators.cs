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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Network.Elastic.Task.Default;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public sealed class BroadcastSlaveTaskDieMultipleEvaluators : DefaultElasticTask
    {
        private static readonly Logger Log = Logger.GetLogger(
            typeof(BroadcastSlaveTaskDieMultipleEvaluators));

        private const int _failProb = 50;
        private readonly Random _rand = new Random();

        [Inject]
        public BroadcastSlaveTaskDieMultipleEvaluators(
            CancellationSource source, IElasticContext context)
            : base(source, context, "Broadcast")
        {
        }

        protected override void Execute(byte[] memento, Workflow workflow)
        {
            if (_rand.Next(100) < _failProb)
            {
                Environment.Exit(0);
            }

            foreach (var op in workflow)
            {
                switch (op.OperatorType)
                {
                    case OperatorType.Broadcast:

                        if (_rand.Next(100) < _failProb)
                        {
                            Environment.Exit(0);
                        }

                        var receiver = workflow.Current as IElasticBroadcast<int>;

                        var rec = receiver.Receive();

                        Log.Log(Level.Info, "Slave has received {0}", rec);

                        if (_rand.Next(100) < _failProb)
                        {
                            Environment.Exit(0);
                        }
                        break;

                    default:
                        break;
                }
            }
        }
    }
}