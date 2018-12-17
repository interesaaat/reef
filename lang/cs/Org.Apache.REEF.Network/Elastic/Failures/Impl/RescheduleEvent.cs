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
using System.Collections.Generic;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    /// <summary>
    /// Reconfigure the execution to work with fewer tasks
    /// </summary>
    [Unstable("0.16", "API may change")]
    public class RescheduleEvent : IReschedule
    {
        public RescheduleEvent(string taskId)
        {
            TaskId = taskId;
            OperatorId = -1;
            FailureResponse = new List<IElasticDriverMessage>();
            RescheduleTaskConfigurations = new Dictionary<string, IList<IConfiguration>>();
            Iteration = Optional<int>.Empty();
        }

        public int FailureEvent
        {
            get { return (int)DefaultFailureStateEvents.Reschedule; }
        }

        public Optional<IFailedTask> FailedTask { get; set; }

        public string TaskId { get; private set; }

        public int OperatorId { get; private set; }

        public Optional<int> Iteration { get; set; }

        public List<IElasticDriverMessage> FailureResponse { get; private set; }

        public Dictionary<string, IList<IConfiguration>> RescheduleTaskConfigurations { get; private set; }

        public bool Reschedule
        {
            get { return RescheduleTaskConfigurations.Count > 0; }
        }
    }
}
