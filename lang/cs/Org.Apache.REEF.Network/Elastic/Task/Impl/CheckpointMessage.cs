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

using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators. This is the class inherited by 
    /// GroupCommunicationMessage but seen by Network Service
    /// </summary>
    internal sealed class CheckpointMessage<T> : GroupCommunicationMessage
    {
        public CheckpointMessage(
           string subscriptionName,
           int operatorId,
           int iteration) : base(subscriptionName, operatorId)
        {
            Payload = default(T);
            Iteration = iteration;
        }

        public T Payload { get; set; }

        public int Iteration { get; set; }

        public bool IsNull
        {
            get { return EqualityComparer<T>.Default.Equals(Payload, default(T)); }
        }

        // The assumption is that messages are immutable therefore there is no need to clone them
        public override object Clone()
        {
            return this;
        }
    }
}