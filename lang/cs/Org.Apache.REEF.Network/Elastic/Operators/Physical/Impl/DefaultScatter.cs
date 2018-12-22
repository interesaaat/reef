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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Operators.Physical.Enum;
using Org.Apache.REEF.Network.Elastic.Failures;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Group Communication Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public sealed class DefaultScatter<T> : DefaultOneToN<T[]>, IElasticScatter<T>
    {
        /// <summary>
        /// Creates a new Broadcast operator.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="topology">The operator topology layer</param>
        [Inject]
        private DefaultScatter(
            [Parameter(typeof(OperatorParameters.OperatorId))] int id,
            [Parameter(typeof(OperatorParameters.Checkpointing))] int level,
            [Parameter(typeof(OperatorParameters.IsLast))] bool isLast,
            ICheckpointableState checkpointableState,
            ScatterTopology topology) : base(id, isLast, checkpointableState, topology)
        {
            OperatorName = Constants.Scatter;
        }

        public void Send(T[] data)
        {
            _topology.TopologyUpdateRequest();

            _position = PositionTracker.InSend;

            int iteration = IteratorReference == null ? 0 : (int)IteratorReference.Current;

            var message = _topology.GetDataMessage(iteration, data);

            Checkpoint(message, message.Iteration);

            _topology.Send(message, CancellationSource);

            _position = PositionTracker.AfterSend;
        }
    }
}
