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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Failures;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Used to configure (Group Communication) operators in REEF driver.
    /// All operators in the same Subscription share similar semantics
    /// and behaviour under failures.
    /// </summary>
    public interface IElasticTaskSetSubscription : IFailureResponse
    {
        string SubscriptionName { get; }

        ElasticOperator RootOperator { get; }

        /// <summary>
        /// This is needed for fault-tolerancy. Failures over an iterative pipeline of operators
        /// have to be propagated through all operators.
        /// <summary>
        bool IsIterative { get; set; }

        IFailureState FailureStatus { get; }

        IElasticTaskSetService Service { get; }

        int GetNextOperatorId();

        IElasticTaskSetSubscription Build();

        bool AddTask(string taskId);

        /// <summary>
        /// Method for implementing different policies for 
        /// triggering the scheduling of subscription's tasks.
        /// </summary>
        bool ScheduleSubscription();

        bool IsMasterTaskContext(IActiveContext activeContext);

        void GetTaskConfiguration(ref ICsConfigurationBuilder builder, int taskId);
    }
}