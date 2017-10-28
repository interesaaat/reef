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

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Messages sent by the driver to operators part of an aggregation ring. 
    /// This message tells the destination node who is the next step in the ring.
    /// </summary>
    internal sealed class ResumeMessagePayload : WithNextMessagePayload
    {
        public ResumeMessagePayload(string nextTaskId, int iteration, string subscriptionName, int operatorId)
            : base(nextTaskId, subscriptionName, operatorId, iteration)
        {
            MessageType = DriverMessageType.Resume;
        }

        internal static DriverMessagePayload From(byte[] data, int offset = 0)
        {
            int length = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            string destination = ByteUtilities.ByteArraysToString(data, offset, length);
            offset += length;

            length = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            string subscription = ByteUtilities.ByteArraysToString(data, offset, length);
            offset += length;

            int operatorId = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            int iteration = BitConverter.ToInt32(data, offset);

            return new ResumeMessagePayload(destination, iteration, subscription, operatorId);
        }
    }
}