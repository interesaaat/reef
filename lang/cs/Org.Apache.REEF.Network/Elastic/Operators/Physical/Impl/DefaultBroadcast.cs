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

using System.Threading;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Config;
using System;
using Org.Apache.REEF.Network.Elastic.Topology.Task;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Group Communication Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public sealed class DefaultBroadcast<T> : IElasticBroadcast<T>
    {
        private readonly IOperatorTopology _topLayer;

        /// <summary>
        /// Creates a new BroadcastReceiver.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="commLayer">The node's communication layer graph</param>
        [Inject]
        private DefaultBroadcast(
            [Parameter(typeof(OperatorsConfiguration.OperatorId))] int id,
            IOperatorTopology topLayer)
        {
            OperatorName = "broadcast";
            OperatorId = id;
            _topLayer = topLayer;
        }

        /// <summary>
        /// Returns the operator identifier.
        /// </summary>
        public int OperatorId { get; private set; }

        public string OperatorName { get; private set; }

        /// <summary>
        /// Receive a message from neighbors broadcasters.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The incoming data</returns>
        public T Receive(CancellationTokenSource cancellationSource)
        {
            var objs = _topLayer.Receive(cancellationSource);

            objs.MoveNext();
            return (T)objs.Current;
        }

        public void Send(T data)
        {
            _topLayer.Send(new object[] { data });
        }

        public void WaitingForRegistration(CancellationTokenSource cancellationSource)
        {
            _topLayer.WaitingForRegistration(cancellationSource);
        }
    }
}
