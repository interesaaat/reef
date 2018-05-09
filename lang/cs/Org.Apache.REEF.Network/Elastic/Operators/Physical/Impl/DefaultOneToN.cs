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
using System;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Group Communication Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public abstract class DefaultOneToN<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultOneToN<>));

        internal readonly OneToNTopology _topology;

        protected volatile PositionTracker _position;

        private readonly bool _isLast;

        /// <summary>
        /// Creates a new Broadcast operator.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="topology">The operator topology layer</param>
        internal DefaultOneToN(int id, int level, bool isLast, OneToNTopology topology)
        {
            OperatorId = id;
            CheckpointLevel = (CheckpointLevel)level;
            _isLast = isLast;
            _topology = topology;
            _position = PositionTracker.Nil;

            OnTaskRescheduled = new Action(() =>
            {
                _topology.JoinTopology();
            });
        }

        /// <summary>
        /// Returns the operator identifier.
        /// </summary>
        public int OperatorId { get; private set; }

        public string OperatorName { get; protected set; }

        private List<GroupCommunicationMessage> CheckpointedMessages { get; set; }

        private CheckpointLevel CheckpointLevel { get; set; }

        public string FailureInfo
        {
            get
            {
                string iteration = IteratorReference == null ? "-1" : IteratorReference.Current.ToString();
                string position = ((int)_position).ToString() + ":";
                string isSending = _topology.IsSending ? "1" : "0";
                return iteration + ":" + position + ":" + isSending;
            }
        }

        public IElasticIterator IteratorReference { protected get;  set; }

        public CancellationTokenSource CancellationSource { get; set; }

        public Action OnTaskRescheduled { get; private set; }

        /// <summary>
        /// Receive a message from neighbors broadcasters.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The incoming data</returns>
        public T Receive()
        {
            _position = PositionTracker.InReceive;

            var received = false;
            DataMessageWithTopology<T> message = null;
            var isIterative = IteratorReference != null;

            while (!received && !CancellationSource.IsCancellationRequested)
            {
                message = _topology.Receive(CancellationSource) as DataMessageWithTopology<T>;

                if (isIterative && message.Iteration < (int)IteratorReference.Current)
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

            if (isIterative)
            {
                IteratorReference.SyncIteration(message.Iteration);
            }

            _position = PositionTracker.AfterReceive;

            return message.Data;
        }

        public void ResetPosition()
        {
            _position = PositionTracker.Nil;
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            LOGGER.Log(Level.Info, "Waiting for task registration for {0} operator", OperatorName);
            _topology.WaitForTaskRegistration(cancellationSource);
        }

        public void WaitCompletionBeforeDisposing()
        {
            _topology.WaitCompletionBeforeDisposing(CancellationSource);
        }

        public void Dispose()
        {
            if (_isLast)
            {
                _topology.SignalSubscriptionComplete();
            }
            _topology.Dispose();
        }

        internal void Checkpoint(GroupCommunicationMessage data, int iteration)
        {
            if (CheckpointLevel > CheckpointLevel.None)
            {
                var state = new CheckpointableObject<GroupCommunicationMessage>()
                {
                    Level = CheckpointLevel,
                    Iteration = iteration
                };

                state.MakeCheckpointable(data);

                _topology.Checkpoint(state);
            }
        }
    }
}
