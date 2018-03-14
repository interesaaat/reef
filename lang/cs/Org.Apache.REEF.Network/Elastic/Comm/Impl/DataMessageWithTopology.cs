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
    internal abstract class DataMessageWithTopology : DataMessage
    {
        public DataMessageWithTopology(string subscriptionName, int operatorId, int iteration)
            : base(subscriptionName, operatorId, iteration)
        {
            Iteration = iteration;
        }

        internal List<TopologyUpdate> TopologyUpdates { get; set; }
    }

    internal sealed class DataMessageWithTopology<T> : DataMessageWithTopology
    {
        public DataMessageWithTopology(
            string subscriptionName,
            int operatorId,
            int iteration, //// For the moment we consider iterations as ints. Maybe this would change in the future
            T data,
            List<TopologyUpdate> updates) : base(subscriptionName, operatorId, iteration)
        {
            Data = data;
            TopologyUpdates = updates;
        }

        public DataMessageWithTopology(
            string subscriptionName,
            int operatorId,
            int iteration, //// For the moment we consider iterations as ints. Maybe this would change in the future
            T data) : this(subscriptionName, operatorId, iteration, data, new List<TopologyUpdate>())
        {
        }

        internal T Data { get; set; }
    }
}