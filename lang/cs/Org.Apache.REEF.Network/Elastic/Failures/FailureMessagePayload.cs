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

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    /// <summary>
    /// Messages sent by the driver to operators part of an aggregation ring. 
    /// This message tells the destination node who is the next step in the ring.
    /// </summary>
    public sealed class FailureMessagePayload : IDriverMessagePayload
    {
        public FailureMessagePayload(string nextTaskId)
        {
            NextTaskId = nextTaskId;
            MessageType = DriverMessageType.Failure;
        }

        public DriverMessageType MessageType { get; private set; }

        public string NextTaskId { get; private set; }

        public byte[] Serialize()
        {
            List<byte[]> buffer = new List<byte[]>();

            byte[] nextBytes = ByteUtilities.StringToByteArrays(NextTaskId);

            buffer.Add(BitConverter.GetBytes(nextBytes.Length));
            buffer.Add(nextBytes);

            return buffer.SelectMany(i => i).ToArray();
        }

        public static IDriverMessagePayload From(byte[] data, int offset = 0)
        {
            int destinationLength = BitConverter.ToInt32(data, offset);
            offset += 4;
            string destination = ByteUtilities.ByteArraysToString(data.Skip(offset).Take(destinationLength).ToArray());

            return new FailureMessagePayload(destination);
        }
    }
}