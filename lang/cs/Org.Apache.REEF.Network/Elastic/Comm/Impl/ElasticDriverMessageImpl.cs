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
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Tang.Exceptions;
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
    public sealed class ElasticDriverMessageImpl : IElasticDriverMessage
    {
        /// <summary>
        /// Create new RingReturnMessage.
        /// </summary>
        /// <param name="destination">The message destination</param>
        /// <param name="message">The message</param>
        public ElasticDriverMessageImpl(
            string destinationTaskId,
            DriverMessagePayload message)
        {
            Destination = destinationTaskId;
            Message = message;
        }

        /// <summary>
        /// The destination task of the message
        public string Destination { get; private set; }

        /// <summary>
        /// Returns the Operator id.
        /// </summary>
        public DriverMessagePayload Message { get; private set; }

        public byte[] Serialize()
        {
            List<byte[]> buffer = new List<byte[]>();

            byte[] destinationBytes = ByteUtilities.StringToByteArrays(Destination);

            buffer.Add(BitConverter.GetBytes(destinationBytes.Length));
            buffer.Add(destinationBytes);
            buffer.Add(BitConverter.GetBytes((short)Message.MessageType));
            buffer.Add(Message.Serialize());

            return buffer.SelectMany(i => i).ToArray();
        }

        public static ElasticDriverMessageImpl From(byte[] data)
        {
            int destinationLength = BitConverter.ToInt32(data, 0);
            int offset = 4;
            string destination = ByteUtilities.ByteArraysToString(data.Skip(offset).Take(destinationLength).ToArray());
            offset += destinationLength;

            DriverMessageType type = (DriverMessageType)BitConverter.ToUInt16(data, offset);
            offset += sizeof(ushort);

            DriverMessagePayload payload = null;

            switch (type)
            {
                case DriverMessageType.Failure:
                    payload = FailureMessagePayload.From(data, offset);
                    break;
                case DriverMessageType.Ring:
                    payload = RingMessagePayload.From(data, offset);
                    break;
                case DriverMessageType.Resume:
                    payload = ResumeMessagePayload.From(data, offset);
                    break;
                default:
                    throw new IllegalStateException("Message type not recognized");
            }

            return new ElasticDriverMessageImpl(destination, payload);
        }
    }
}