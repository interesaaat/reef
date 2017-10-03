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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    public class DriverMessageHandler : IDriverMessageHandler
    {
        [Inject]
        public DriverMessageHandler()
        {
        }

        internal ConcurrentDictionary<string, ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology>> DriverMessageObservers { get; set; }

        public void Handle(IDriverMessage value)
        {
            if (value.Message.IsPresent())
            {
                var edm = ElasticDriverMessageImpl.From(value.Message.Value);
                var id = NodeObserverIdentifier.FromMessage(edm.Message);
                ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology> observers;
                DriverAwareOperatorTopology operatorObserver;

                if (!DriverMessageObservers.TryGetValue(edm.Destination, out observers))
                {
                    throw new KeyNotFoundException("Unable to find registered task Observer for source Task " +
                        edm.Destination + ".");
                }

                if (!observers.TryGetValue(id, out operatorObserver))
                {
                    throw new KeyNotFoundException("Unable to find registered Operator Topology for Subscription " +
                        edm.Message.SubscriptionName + " operator " + edm.Message.OperatorId);
                }

                operatorObserver.OnNext(edm.Message);
            }
            else
            {
                throw new IllegalStateException("Received message with no payload");
            }
        }
    }
}