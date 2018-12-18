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
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Config;
using System;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Network.Elastic.Operators.Physical.Enum;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Group Communication Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public sealed class DefaultAggregationRing<T> : IElasticAggregationRing<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultAggregationRing<>));

        private readonly ICheckpointableState _checkpointableState;
        private readonly AggregationRingTopology _topology;
        private volatile PositionTracker _position;

        /// <summary>
        /// Creates a new BroadcastReceiver.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="topology">The operator topology layer</param>
        [Inject]
        private DefaultAggregationRing(
            [Parameter(typeof(OperatorParameters.OperatorId))] int id,
            [Parameter(typeof(OperatorParameters.Checkpointing))] int level,
            AggregationRingTopology topology,
            ICheckpointableState checkpointableState)
        {
            OperatorName = Constants.AggregationRing;
            OperatorId = id;
            CheckpointLevel = (CheckpointLevel)level;
            _checkpointableState = checkpointableState;
            _position = PositionTracker.Nil;

            _topology = topology;
            _topology.Operator = this;
            OnTaskRescheduled = new Action(() => { });
        }

        /// <summary>
        /// Returns the operator identifier.
        /// </summary>
        public int OperatorId { get; private set; }

        public string OperatorName { get; private set; }

        public string FailureInfo
        {
            get
            {
                string iteration = IteratorReference == null ? "-1" : IteratorReference.Current.ToString();
                return ((int)_position).ToString() + ":" + iteration;
            }
        }

        internal CheckpointLevel CheckpointLevel { get; set; }

        public IElasticIterator IteratorReference { private get;  set; }

        public CancellationTokenSource CancellationSource { get; set; }

        public Action OnTaskRescheduled { get; private set; }

        /// <summary>
        /// Receive a message from neighbors broadcasters.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The incoming data</returns>
        public T Receive()
        {
            _topology.JoinTopology();

            _position = PositionTracker.InReceive;

            var received = false;
            DataMessage<T> message = null;

            while (!received && !CancellationSource.IsCancellationRequested)
            {
                message = _topology.Receive(CancellationSource) as DataMessage<T>;

                if (message.Iteration < (int)IteratorReference.Current)
                {
                    LOGGER.Log(Level.Warning, "Received message for iteration {0} but I am already in iteration {1}: ignoring", message.Iteration, (int)IteratorReference.Current);
                }
                else
                {
                    received = true;
                }
            }

            if (message == null)
            {
                throw new OperationCanceledException("Impossible to receive messages: operation canceled");
            }

            IteratorReference.SyncIteration(message.Iteration);

            _position = PositionTracker.AfterReceiveBeforeSend;

            return message.Data;
        }

        public void Send(T data)
        {
            _position = PositionTracker.InSend;

            var message = new DataMessage<T>(_topology.SubscriptionName, OperatorId, (int)IteratorReference.Current, data);

            Checkpoint(message, message.Iteration);

            _topology.Send(message, CancellationSource);

            _position = PositionTracker.AfterSendBeforeReceive;
        }

        public void ResetPosition()
        {
            _position = PositionTracker.Nil;
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            LOGGER.Log(Level.Info, "Waiting for task registration for ring operator");
            _topology.WaitForTaskRegistration(cancellationSource);
        }

        public void WaitCompletionBeforeDisposing()
        {
            if (CheckpointLevel > CheckpointLevel.None)
            {
                _topology.WaitCompletionBeforeDisposing(CancellationSource);
            }
        }

        public void Dispose()
        {
            _topology.Dispose();
        }

        internal void Checkpoint(GroupCommunicationMessage data, int iteration)
        {
            if (CheckpointLevel > CheckpointLevel.None)
            {
                var state = _checkpointableState.From(iteration);

                state.MakeCheckpointable(data);
                _topology.Checkpoint(state);
            }
        }
    }
}
