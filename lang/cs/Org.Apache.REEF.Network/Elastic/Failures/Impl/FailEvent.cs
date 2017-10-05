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

using Org.Apache.REEF.Network.Elastic.Comm;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    /// <summary>
    /// Reconfigure the execution to work with fewer tasks
    /// </summary>
    public class FailEvent : IFailureEvent
    {
        public FailEvent(string taskId)
        {
            TaskId = taskId;
            FailureResponse = new List<IElasticDriverMessage>();
        }

        public int FailureEvent
        {
            get { return (int)DefaultFailureStateEvents.Fail; }
        }

        public string TaskId { get; private set; }

        public int OperatorId
        {
            get { return -1; }
        }

        public List<IElasticDriverMessage> FailureResponse { get; private set; }
    }
}
