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

using Org.Apache.REEF.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Messages sent by the driver to operators part of an aggregation ring. 
    /// This message tells the destination node who is the next step in the ring.
    /// </summary>
    internal sealed class TopologyMessagePayload : DriverMessagePayload
    {
        public TopologyMessagePayload(IList<string> updates, bool toRemove, string subscriptionName, int operatorId, int iteration)
            : base(subscriptionName, operatorId, iteration)
        {
            MessageType = DriverMessageType.Topology;
            TopologyUpdates = updates;
            ToRemove = toRemove;
        }

        internal IList<string> TopologyUpdates { get; private set; }

        internal bool ToRemove { get; private set; }

        internal override byte[] Serialize()
        {
            byte[] subscriptionBytes = ByteUtilities.StringToByteArrays(SubscriptionName);
            int offset = 0;
            var totalLengthUpdates = TopologyUpdates.Sum(x => x.Length) + (TopologyUpdates.Count * sizeof(int));
            byte[] buffer = new byte[sizeof(int) + totalLengthUpdates + sizeof(int) + subscriptionBytes.Length + sizeof(bool) + sizeof(int) + sizeof(int)];
            byte[] tmpBuffer;

            Buffer.BlockCopy(BitConverter.GetBytes(totalLengthUpdates), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);
            foreach (var value in TopologyUpdates)
            {
                tmpBuffer = ByteUtilities.StringToByteArrays(value);
                Buffer.BlockCopy(BitConverter.GetBytes(tmpBuffer.Length), 0, buffer, offset, sizeof(int));
                offset += sizeof(int);
                Buffer.BlockCopy(tmpBuffer, 0, buffer, offset, tmpBuffer.Length);
                offset += tmpBuffer.Length;
            }

            Buffer.BlockCopy(BitConverter.GetBytes(subscriptionBytes.Length), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(subscriptionBytes, 0, buffer, offset, subscriptionBytes.Length);
            offset += subscriptionBytes.Length;

            Buffer.BlockCopy(BitConverter.GetBytes(ToRemove), 0, buffer, offset, sizeof(bool));
            offset += sizeof(bool);
            Buffer.BlockCopy(BitConverter.GetBytes(OperatorId), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(Iteration), 0, buffer, offset, sizeof(int));

            return buffer;
        }

        internal static DriverMessagePayload From(byte[] data, int offset = 0)
        {
            int length = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            IList<string> updates = Deserialize(data, length, offset);
            offset += length;

            length = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            string subscription = ByteUtilities.ByteArraysToString(data, offset, length);
            offset += length;

            bool toRemove = BitConverter.ToBoolean(data, offset);
            offset += sizeof(bool);
            int operatorId = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            int iteration = BitConverter.ToInt32(data, offset);

            return new TopologyMessagePayload(updates, toRemove, subscription, operatorId, iteration);
        }

        private static IList<string> Deserialize(byte[] data, int totLength, int start)
        {
            var result = new List<string>();
            var length = 0;
            var offset = 0;
            string value;

            while (offset < totLength)
            {
                length = BitConverter.ToInt32(data, start + offset);
                offset += sizeof(int);
                value = ByteUtilities.ByteArraysToString(data, start + offset, length);
                offset += length;
                result.Add(value);
            }

            return result;
        }
    }
}