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

using System.Collections.Generic;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Elastic.Config
{
    public sealed class ElasticServiceConfigurationOptions
    {
        [NamedParameter("Number of Evaluators")]
        public class NumEvaluators : Name<int>
        {
        }

        [NamedParameter("Number of Servers")]
        public class NumServers : Name<int>
        {
        }

        [NamedParameter("Number of Workes")]
        public class NumWorkers : Name<int>
        {
        }

        [NamedParameter(Documentation = "Number of retry when a failure occurs", DefaultValue = "1")]
        public class NumRetry : Name<int>
        {
        }

        [NamedParameter(Documentation = "Starting port for TcpPortProvider", DefaultValue = "8900")]
        public class StartingPort : Name<int>
        {
        }

        [NamedParameter(Documentation = "Port Range count for TcpPortProvider", DefaultValue = "1000")]
        public class PortRange : Name<int>
        {
        }

        [NamedParameter("Driver identifier")]
        public class DriverId : Name<string>
        {
        }

        [NamedParameter("Default Group name", defaultValue: "Subscription1")]
        public class DefaultSubscriptionName : Name<string>
        {
        }

        [NamedParameter("Number of tasks", defaultValue: "5")]
        public class NumberOfTasks : Name<int>
        {
        }

        [NamedParameter("Serialized subscriptions configuration")]
        public class SerializedSubscriptionConfigs : Name<ISet<string>>
        {
        }

        [NamedParameter("Number of checkpoints to store per operator", defaultValue: "1")]
        public class NumCheckpoints : Name<int>
        {
        }
    }
}
