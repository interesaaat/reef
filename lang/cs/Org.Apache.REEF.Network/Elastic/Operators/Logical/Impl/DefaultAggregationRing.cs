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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities.Logging;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using System.Linq;
using System;
using Org.Apache.REEF.Network.Elastic.Task;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Broadcast operator implementation.
    /// </summary>
    class DefaultAggregationRing<T> : ElasticOperatorWithDefaultDispatcher, IElasticAggregationRing
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultAggregationRing<>));

        private HashSet<string> _currentWaitingList;
        private HashSet<string> _nextWaitingList;
        private HashSet<string> _tasksInRing;
        private LinkedList<string> _ring;
        private LinkedList<string> _prevRing;
        private string _coordinatorTaskId;
        private volatile string _tokenPosition;

        private readonly object _lock;

        public DefaultAggregationRing(
            int coordinatorId,
            ElasticOperator prev,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null,
                prev,
                new FullConnectedTopology(coordinatorId),
                failureMachine,
                checkpointLevel,
                configurations)
        {
            MasterId = coordinatorId;
            OperatorName = Constants.AggregationRing;
            _coordinatorTaskId = Utils.BuildTaskId(Subscription.SubscriptionName, MasterId);

            _currentWaitingList = new HashSet<string>();
            _nextWaitingList = new HashSet<string>();
            _tasksInRing = new HashSet<string> { { _coordinatorTaskId } };
            _ring = new LinkedList<string>();
            _ring.AddLast(_coordinatorTaskId);
            _tokenPosition = string.Empty;

            _lock = new object();
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder.BindImplementation(GenericType<IElasticBasicOperator<T>>.Class, GenericType<Physical.Impl.DefaultAggregationRing<T>>.Class);
            SetMessageType(typeof(Physical.Impl.DefaultAggregationRing<T>), ref confBuilder);
        }

        protected override ISet<DriverMessage> ReactOnTaskMessage(ITaskMessage message)
        {
            var msgReceived = (RingTaskMessageType)BitConverter.ToUInt16(message.Message, 0);

            switch (msgReceived)
            {
                case RingTaskMessageType.WaitForToken:
                   lock (_lock)
                   {
                        if (_currentWaitingList.Contains(message.TaskId) || _tasksInRing.Contains(message.TaskId))
                        {
                            _nextWaitingList.Add(message.TaskId);
                        }
                        else
                        {
                            _currentWaitingList.Add(message.TaskId);
                        }
                    }

                    var messages = new HashSet<DriverMessage>();

                    SubmitNextNodes(ref messages);

                    return messages;
                case RingTaskMessageType.TokenReceived:
                    _tokenPosition = message.TaskId;
                    return new HashSet<DriverMessage>();
                default:
                    return null;
            }
        }

        private void SubmitNextNodes(ref HashSet<DriverMessage> messages)
        {
            lock (_lock)
            {
                while (_currentWaitingList.Count > 0)
                {
                    var enumerator = _currentWaitingList.Take(1);
                    foreach (var nextTask in enumerator)
                    {
                        var dest = _ring.Last.Value;
                        var data = new RingMessagePayload(nextTask);
                        var returnMessage = new DriverMessage(dest, data);

                        messages.Add(returnMessage);
                        _ring.AddLast(nextTask);
                        _tasksInRing.Add(nextTask);
                        _currentWaitingList.Remove(nextTask);
                    }
                }

                if (_failureMachine.NumOfDataPoints - _failureMachine.NumOfFailedDataPoints <= _ring.Count)
                {
                    var dest = _ring.Last.Value;
                    var data = new RingMessagePayload(_ring.First.Value);
                    var returnMessage = new DriverMessage(dest, data);

                    messages.Add(returnMessage);
                    LOGGER.Log(Level.Info, "Ring is closed:\n {0}->{1}", string.Join("->", _ring), _ring.First.Value);

                    _prevRing = _ring;
                    _ring = new LinkedList<string>();
                    _ring.AddLast(_prevRing.First.Value);
                    _currentWaitingList = _nextWaitingList;
                    _nextWaitingList = new HashSet<string>();
                    _tasksInRing = new HashSet<string> { { _coordinatorTaskId } };

                    foreach (var task in _currentWaitingList)
                    {
                        _tasksInRing.Add(task);
                    }
                }
            }

            // Continuously build the ring until there is some node waiting
            if (_currentWaitingList.Count > 0)
            {
                SubmitNextNodes(ref messages); 
            }
        }
    }
}
