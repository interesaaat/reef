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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Handler for incoming messages from the driver.
    /// </summary>
    internal sealed class ElasticDriverMessageHandler : IDriverMessageHandler
    {
        /// <summary>
        /// Injectable constructor.
        /// </summary>
        [Inject]
        private ElasticDriverMessageHandler()
        {
            DriverMessageObservers = new ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology>();
        }

        /// <summary>
        /// Observers of incoming messages from the driver.
        /// </summary>
        internal ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology> DriverMessageObservers { get; set; }

        /// <summary>
        /// Handle an incoming message.
        /// </summary>
        /// <param name="message">The message from the driver</param>
        public void Handle(IDriverMessage message)
        {

            if (!message.Message.IsPresent())
            {
                throw new IllegalStateException("Received message with no payload.");
            }

            var edm = ElasticDriverMessageImpl.From(message.Message.Value);
            var id = NodeObserverIdentifier.FromMessage(edm.Message);
            DriverAwareOperatorTopology operatorObserver;

            if (!DriverMessageObservers.TryGetValue(id, out operatorObserver))
            {
                throw new KeyNotFoundException("Unable to find registered operator topology for stage " +
                    edm.Message.StageName + " operator " + edm.Message.OperatorId);
            }

            operatorObserver.OnNext(edm.Message);
        }
    }
}