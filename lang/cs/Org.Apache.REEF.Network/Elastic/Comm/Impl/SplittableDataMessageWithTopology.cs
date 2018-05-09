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
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    internal abstract class SplittableDataMessageWithTopology : DataMessageWithTopology
    {
        public SplittableDataMessageWithTopology(string subscriptionName, int operatorId, int iteration, List<TopologyUpdate> updates)
            : base(subscriptionName, operatorId, iteration)
        {
            TopologyUpdates = updates;
        }

        public abstract IEnumerable<DataMessageWithTopology> GetSplits(int numElements);
    }

    internal sealed class SplittableDataMessageWithTopology<T> : SplittableDataMessageWithTopology
    {
        public SplittableDataMessageWithTopology(
            string subscriptionName,
            int operatorId,
            int iteration, //// For the moment we consider iterations as ints. Maybe this would change in the future
            T[] data,
            List<TopologyUpdate> updates) : base(subscriptionName, operatorId, iteration, updates)
        {
            Data = data;
        }

        public SplittableDataMessageWithTopology(
            string subscriptionName,
            int operatorId,
            int iteration, //// For the moment we consider iterations as ints. Maybe this would change in the future
            T[] data) : this(subscriptionName, operatorId, iteration, data, new List<TopologyUpdate>())
        {
        }

        internal T[] Data { get; set; }

        public override IEnumerable<DataMessageWithTopology> GetSplits(int numElements)
        {
            int size = Data.Length / numElements;

            for (var i = 0; i < numElements; i++)
            {
                yield return new DataMessageWithTopology<T[]>(SubscriptionName, OperatorId, Iteration, Data.Skip(i * size).Take(size).ToArray(), TopologyUpdates);
            }
        }
    }
}