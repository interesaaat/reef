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
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using System;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Group Communication Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public sealed class DefaultAggregationRing<T> : IElasticAggregationRing<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultAggregationRing<>));

        private readonly AggregationRingTopology _topology;
        private PositionTracker _position;

        /// <summary>
        /// Creates a new BroadcastReceiver.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="topology">The operator topology layer</param>
        [Inject]
        private DefaultAggregationRing(
            [Parameter(typeof(OperatorId))] int id,
            [Parameter(typeof(Checkpointing))] int level,
            AggregationRingTopology topology)
        {
            OperatorName = Constants.AggregationRing;
            OperatorId = id;
            CheckpointLevel = (CheckpointLevel)level;
            _position = PositionTracker.Nil;

            _topology = topology;
            _topology.Operator = this;
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

        /// <summary>
        /// Receive a message from neighbors broadcasters.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The incoming data</returns>
        public T Receive()
        {
            _topology.JoinTheRing((int)IteratorReference.Current);

            _position = PositionTracker.InReceive;

            var received = false;
            DataMessage<T> message = null;

            while (!received && !CancellationSource.IsCancellationRequested)
            {
                message = _topology.Receive(CancellationSource, (int)IteratorReference.Current) as DataMessage<T>;

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
            var messages = new GroupCommunicationMessage[] { message };

            Checkpoint(messages, message.Iteration);

            _topology.Send(messages, CancellationSource);

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
                _topology.WaitCompletionBeforeDisposing(CancellationSource);
            }
        }

        public void Dispose()
        {
            _topology.Dispose();
        }

        internal void Checkpoint(GroupCommunicationMessage[] data, int iteration)
        {
            if (CheckpointLevel > CheckpointLevel.None)
            {
                var state = new CheckpointableObject<GroupCommunicationMessage[]>()
                {
                    Level = CheckpointLevel,
                    Iteration = iteration
                };

                state.MakeCheckpointable(data);

                _topology.Checkpoint(state);
            }
        }
    }

    static class Extensions
    {
        public static IList<T> Clone<T>(this IList<T> listToClone) where T : ICloneable
        {
            return listToClone.Select(item => (T)item.Clone()).ToList();
        }
    }
}
