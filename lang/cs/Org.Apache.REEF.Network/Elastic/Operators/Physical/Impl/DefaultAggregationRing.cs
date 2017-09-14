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
using Org.Apache.REEF.Network.Elastic.Config;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Group Communication Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public sealed class DefaultAggregationRing<T> : CheckpointingOperator<List<GroupCommunicationMessage>>, IElasticAggregationRing<T>
    {
        private readonly AggregationRingTopology _topology;
        private PositionTracker _position;
        private int _iterationNumber = 0;

        /// <summary>
        /// Creates a new BroadcastReceiver.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="topology">The operator topology layer</param>
        [Inject]
        private DefaultAggregationRing(
            [Parameter(typeof(OperatorParameters.OperatorId))] int id,
            [Parameter(typeof(OperatorParameters.Checkpointing))] int level,
            AggregationRingTopology topology)
        {
            OperatorName = Constants.AggregationRing;
            OperatorId = id;
            CheckpointLevel = CheckpointLevel.Memory; ////(CheckpointLevel)level;
            _topology = topology;
            _position = PositionTracker.Nil;
        }

        /// <summary>
        /// Returns the operator identifier.
        /// </summary>
        public int OperatorId { get; private set; }

        public string OperatorName { get; private set; }

        private CheckpointLevel CheckpointLevel { get; set; }

        public string FailureInfo
        {
            get { return ((int)_position).ToString() + ":" + _iterationNumber; }
        }

        /// <summary>
        /// Receive a message from neighbors broadcasters.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The incoming data</returns>
        public T Receive(CancellationTokenSource cancellationSource)
        {
            _iterationNumber++;
            _position = PositionTracker.InReceive;
            _topology.JoinTheRing();

            var objs = _topology.Receive(cancellationSource);

            objs.MoveNext();
            var message = objs.Current as DataMessage<T>;

            _topology.TokenReceived(_iterationNumber);
            _position = PositionTracker.AfterReceiveBeforeSend;

            return message.Data;
        }

        public void Send(T data, CancellationTokenSource cancellationSource)
        {
            _position = PositionTracker.InSend;
            var message = new DataMessage<T>(_topology.SubscriptionName, OperatorId, data);
            var messages = new List<GroupCommunicationMessage> { message };

            Checkpoint(messages);

            _topology.Send(new List<GroupCommunicationMessage> { message }, cancellationSource);
            _position = PositionTracker.AfterSendBeforeReceive;
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            _topology.WaitForTaskRegistration(cancellationSource);
        }

        public void WaitCompletionBeforeDisposing()
        {
            if (CheckpointLevel > CheckpointLevel.None)
            {
                _topology.WaitCompletionBeforeDisposing();
            }
        }

        public void Dispose()
        {
            _topology.Dispose();
        }

        internal override void Checkpoint(List<GroupCommunicationMessage> data)
        {
            switch (CheckpointLevel)
            {
                case CheckpointLevel.Memory:
                    _topology.CheckpointedData = new List<GroupCommunicationMessage>(data);
                    break;
                default: break;
            }
        }
    }
}
