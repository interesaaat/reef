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

using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    /// <summary>
    /// Messages sent by the driver to operators part of an aggregation ring. 
    /// This message tells the destination node who is the next step in the ring.
    /// </summary>
    internal sealed class RingMessagePayload : IDriverMessagePayload
    {
        public RingMessagePayload(string nextTaskId, string subscriptionName, int operatorId)
            : base(subscriptionName, operatorId)
        {
            NextTaskId = nextTaskId;
            MessageType = DriverMessageType.Ring;
        }

        internal string NextTaskId { get; private set; }

        internal override byte[] Serialize()
        {
            byte[] nextBytes = ByteUtilities.StringToByteArrays(NextTaskId);
            byte[] subscription = ByteUtilities.StringToByteArrays(SubscriptionName);
            int offset = 0;

            byte[] buffer = new byte[sizeof(int) + nextBytes.Length + sizeof(int) + subscription.Length + sizeof(int)];

            Buffer.BlockCopy(BitConverter.GetBytes(nextBytes.Length), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(nextBytes, 0, buffer, offset, nextBytes.Length);
            offset += nextBytes.Length;

            Buffer.BlockCopy(BitConverter.GetBytes(subscription.Length), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(subscription, 0, buffer, offset, subscription.Length);
            offset += subscription.Length;

            Buffer.BlockCopy(BitConverter.GetBytes(OperatorId), 0, buffer, offset, sizeof(int));

            return buffer;
        }

        internal static IDriverMessagePayload From(byte[] data, int offset = 0)
        {
            int length = BitConverter.ToInt32(data, offset);
            offset += 4;
            string destination = ByteUtilities.ByteArraysToString(data, offset, length);
            offset += length;

            length = BitConverter.ToInt32(data, offset);
            offset += 4;
            string subscription = ByteUtilities.ByteArraysToString(data, offset, length);
            offset += length;

            int operatorId = BitConverter.ToInt32(data, offset);

            return new RingMessagePayload(destination, subscription, operatorId);
        }

        public override object Clone()
        {
            return this;
        }
    }
}