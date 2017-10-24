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

using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities;
using System;

namespace Org.Apache.REEF.Network.Elastic.Comm
{
    /// <summary>
    /// Payload of Driver messages.
    /// </summary>
    public abstract class DriverMessagePayload : GroupCommunicationMessage
    {
        public DriverMessagePayload(string subscriptionName, int operatorId, int iteration)
            : base(subscriptionName, operatorId)
        {
            Iteration = iteration;
        }

        /// <summary>
        /// The type of payload
        /// </summary>
        internal DriverMessageType MessageType { get; set; }

        internal int Iteration { get; private set; }

        /// <summary>
        /// Utility method to serialize the payload for communication
        /// </summary>
        internal abstract byte[] Serialize();

        public override object Clone()
        {
            return this;
        }
    }
}