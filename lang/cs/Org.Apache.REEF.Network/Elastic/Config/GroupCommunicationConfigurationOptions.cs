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
    public sealed class GroupCommunicationConfigurationOptions
    {
        [NamedParameter("Serialized operator configuration")]
        public class SerializedOperatorConfigs : Name<ISet<string>>
        {
        }

        [NamedParameter("Id of root task in operator topology", defaultValue: "-1")]
        public class TopologyRootTaskId : Name<int>
        {
        }

        [NamedParameter("Ids of child tasks in operator topology")]
        public class TopologyChildTaskIds : Name<ISet<int>>
        {
        }

        [NamedParameter("Name of the subscriptions")]
        public class SubscriptionName : Name<string>
        {
        }

        [NamedParameter("Timeout for receiving data", defaultValue: "60000")]
        public class Timeout : Name<int>
        {
        }

        [NamedParameter("Timeout for disposing operators when messages are still in queue", defaultValue: "60000")]
        public class DisposeTimeout : Name<int>
        {
        }

        /// <summary>
        /// Each Communication group needs to check and wait until all the other nodes in the group are registered to the NameServer
        /// Sleep time is set between each retry. 
        /// </summary>
        [NamedParameter("sleep time to wait for nodes to be registered", defaultValue: "2000")]
        internal sealed class SleepTimeWaitingForRegistration : Name<int>
        {
        }

        /// <summary>
        /// Each Communication group needs to check and wait until all the other nodes in the group are registered to the NameServer
        /// </summary>
        /// <remarks>
        /// If a node is waiting for others that need to download data, the waiting time could be long. 
        /// As we can use cancellation token to cancel the waiting for registration, setting this number higher should be OK.
        /// Current default sleep time is 2000ms. Default retry is 900. Total is 1800s, that is 30 min.
        /// </remarks>
        [NamedParameter("Retry times to wait for nodes to be registered", defaultValue: "10")]
        internal sealed class RetryCountWaitingForRegistration : Name<int>
        {
        }
    }
}
