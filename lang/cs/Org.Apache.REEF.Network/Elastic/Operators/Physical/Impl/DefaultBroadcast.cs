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
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using System;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Group Communication Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public sealed class DefaultBroadcast<T> : IElasticBroadcast<T>
    {
        private readonly BroadcastTopology _topology;
        private PositionTracker _position;

        /// <summary>
        /// Creates a new BroadcastReceiver.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="topology">The operator topology layer</param>
        [Inject]
        private DefaultBroadcast(
            [Parameter(typeof(OperatorId))] int id,
            [Parameter(typeof(Checkpointing))] int level,
            BroadcastTopology topology)
        {
            OperatorName = Constants.Broadcast;
            OperatorId = id;
            CheckpointLevel = (CheckpointLevel)level;
            _topology = topology;
            _position = PositionTracker.Nil;
        }

        /// <summary>
        /// Returns the operator identifier.
        /// </summary>
        public int OperatorId { get; private set; }

        public string OperatorName { get; private set; }

        private List<GroupCommunicationMessage> CheckpointedMessages { get; set; }

        private CheckpointLevel CheckpointLevel { get; set; }

        public string FailureInfo
        {
            get { return _position.ToString(); }
        }

        public IElasticIterator IteratorReference { private get;  set; }

        public CancellationTokenSource CancellationSource { get; set; }

        /// <summary>
        /// Receive a message from neighbors broadcasters.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The incoming data</returns>
        public T Receive()
        {
            _position = PositionTracker.InReceive;
            var objs = _topology.Receive(CancellationSource);

            objs.MoveNext();
            var message = objs.Current as DataMessage<T>;

            _position = PositionTracker.AfterReceiveBeforeSend;

            return message.Data;
        }

        public void Send(T data)
        {
            _position = PositionTracker.InSend;

            var message = new DataMessage<T>(_topology.SubscriptionName, OperatorId, (int)IteratorReference.Current, data);

            _topology.Send(new GroupCommunicationMessage[] { message }, CancellationSource);

            _position = PositionTracker.AfterSendBeforeReceive;
        }

        public void ResetPosition()
        {
            _position = PositionTracker.Nil;
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
    }
}
