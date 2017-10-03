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

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators. This is the class inherited by 
    /// GroupCommunicationMessage but seen by Network Service
    /// </summary>
    public abstract class GroupCommunicationMessage : ICloneable
    {
        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="messageType">The type of the GC message</param>
        protected GroupCommunicationMessage(
            string subscriptionName,
            int operatorId)
        {
            SubscriptionName = subscriptionName;
            OperatorId = operatorId;
        }

        /// <summary>
        /// Returns the Subscription
        internal string SubscriptionName { get; private set; }

        /// <summary>
        /// Returns the Operator id.
        /// </summary>
        internal int OperatorId { get; private set; }

        public virtual object Clone()
        {
            return this;
        }
    }
}