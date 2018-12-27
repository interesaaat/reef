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

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators. This is the class inherited by 
    /// GroupCommunicationMessage but seen by Network Service
    /// </summary>
    internal sealed class ControlMessage<T> : GroupCommunicationMessage
    {
        public ControlMessage(
           string stageName,
           int operatorId,
           ControlMessageType type) : base(stageName, operatorId)
        {
            Type = Type;
            Payload = default(T);
        }

        internal ControlMessageType Type { get; set; }

        internal bool WithPayload
        {
            get { return EqualityComparer<T>.Default.Equals(Payload, default(T)); }
        }

        internal T Payload { get; set; }

        // The assumption is that messages are immutable therefore there is no need to clone them
        override public object Clone()
        {
            return this;
        }
    }
}