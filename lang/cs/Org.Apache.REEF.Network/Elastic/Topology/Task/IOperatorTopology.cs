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

using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Task.Impl;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Tang.Annotations;
using System.Collections.Generic;
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Topology.Task
{
    /// <summary>
    /// Contains an Operator's topology graph.
    /// Used to send or receive messages to/from operators in the same
    /// Communication Group.
    /// </summary>
    internal interface IOperatorTopology<T> : IRegistration
    {
        /// <summary>
        /// Sends the data to neighbours.
        /// </summary>
        /// <param name="message">The data to send</param>
        /// <param name="type">The message type</param>
        void Send(IList<T> data);

        IEnumerator<T> Receive(CancellationTokenSource cancellationSource);

        string SubscriptionName { get; }

        int OperatorId { get; }
    }
}
