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

using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Comm
{
    /// <summary>
    /// Payload for messages going from the driver to tasks.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public abstract class DriverMessagePayload : GroupCommunicationMessage
    {
        /// <summary>
        /// Construct a payload for messages created at the driver and directed to tasks.
        /// </summary>
        /// <param name="subscriptionName">The name of the subsription</param>
        /// <param name="operatorId">The id of the operator within the subscription</param>
        /// <param name="iteration">The iteration number in which the message is sent</param>
        public DriverMessagePayload(string subscriptionName, int operatorId, int iteration)
            : base(subscriptionName, operatorId)
        {
            Iteration = iteration;
        }

        /// <summary>
        /// The type of payload.
        /// </summary>
        internal DriverMessageType MessageType { get; set; }

        /// <summary>
        /// The iteration number in which the message is sent.
        /// </summary>
        internal int Iteration { get; private set; }

        /// <summary>
        /// Utility method to serialize the payload for communication.
        /// </summary>
        /// <returns>The serialized payload</returns>
        internal abstract byte[] Serialize();
    }

    /// <summary>
    /// Possible types of driver message payloads.
    /// </summary>
    [Unstable("0.16", "Types may change")]
    internal enum DriverMessageType : ushort
    {
        Ring = 1,

        Resume = 2,

        Topology = 3
    }
}