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
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Driver.Policy;
using System.Globalization;
using Org.Apache.REEF.Network.Elastic.Topology.Impl;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    class Empty : ElasticOperator
    {
        private const string _operator = "empty";

        public Empty(IElasticTaskSetSubscription subscription) : base(subscription)
        {
            _policy = PolicyLevel.Ignore;
            _topology = new EmptyTopology();
            _id = GetSubscription.GetNextOperatorId();
        }

        public override void OnStopAndRecompute()
        {
            throw new NotImplementedException();
        }

        public override void OnStopAndResubmit()
        {
            throw new NotImplementedException();
        }
    }
}
