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

using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    /// <summary>
    /// Reconfigure the execution to work with fewer tasks.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public class ReconfigureEvent : IReconfigure
    {
        /// <summary>
        /// Constructor for a reconfigure event.
        /// </summary>
        /// <param name="failedTask">The failed task</param>
        /// <param name="opertorId">The operator identifier in which the event was detected</param>
        public ReconfigureEvent(IFailedTask failedTask, int opertorId)
        {
            FailedTask = Optional<IFailedTask>.Of(failedTask);
            OperatorId = opertorId;
            FailureResponse = new List<IElasticDriverMessage>();
            Iteration = Optional<int>.Empty();
            TaskId = failedTask.Id;
        }

        /// <summary>
        /// The event / action raised by the transition to the new failure state.
        /// </summary>
        public int FailureEvent
        {
            get { return (int)DefaultFailureStateEvents.Reconfigure; }
        }

        /// <summary>
        /// The failed task triggering the event.
        /// </summary>
        public Optional<IFailedTask> FailedTask { get; set; }

        /// <summary>
        /// The iteration in which the failure is rised.
        /// </summary>
        public Optional<int> Iteration { get; set; }

        /// <summary>
        /// The identifier of the task triggering the event.
        /// </summary>
        public string TaskId { get; private set; }

        /// <summary>
        /// The opeartor id in which the failure is rised.
        /// </summary>
        public int OperatorId { get; private set; }

        /// <summary>
        /// The response message generated to react to the failure event.
        /// </summary>
        public List<IElasticDriverMessage> FailureResponse { get; private set; }
    }
}
