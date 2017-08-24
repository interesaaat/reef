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
using Org.Apache.REEF.Network.Elastic.Topology.Impl;
using System.Collections.Concurrent;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities.Logging;
using System.Collections.Generic;
using System;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Broadcast operator implementation.
    /// </summary>
    class DefaultAggregationRing<T> : ElasticOperatorWithDefaultDispatcher, IElasticAggregationRing
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultAggregationRing<>));

        private ConcurrentDictionary<string, byte> _currentWaitingList;
        private ConcurrentDictionary<string, byte> _nextWaitingList;
        private ConcurrentDictionary<string, byte> _tasksInRing;
        private LinkedList<string> _ring;
        private LinkedList<string> _prevRing;
        private string _coordinatorTaskId;

        private readonly object _ringLock;
        private readonly object _waitingListLock;

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

            _currentWaitingList = new ConcurrentDictionary<string, byte>();
            _nextWaitingList = new ConcurrentDictionary<string, byte>();
            _tasksInRing = new ConcurrentDictionary<string, byte>();
            _tasksInRing.AddOrUpdate(_coordinatorTaskId, 0, (task, value) => value);
            _ring = new LinkedList<string>();
            _ring.AddLast(_coordinatorTaskId);
           
            _ringLock = new object();
            _waitingListLock = new object();
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder.BindImplementation(GenericType<IElasticBasicOperator<T>>.Class, GenericType<Physical.Impl.DefaultAggregationRing<T>>.Class);
            SetMessageType(typeof(Physical.Impl.DefaultAggregationRing<T>), ref confBuilder);
        }

        protected override ISet<RingReturnMessage> ReactOnTaskMessage(ITaskMessage message)
        {
            var msgReceived = ByteUtilities.ByteArraysToString(message.Message);

            switch (msgReceived)
            {
                case Constants.AggregationRing:
                    lock (_waitingListLock)
                    {
                        if (_currentWaitingList.ContainsKey(message.TaskId) || _tasksInRing.ContainsKey(message.TaskId))
                        {
                            _nextWaitingList.AddOrUpdate(message.TaskId, 0, (task, value) => value);
                        }
                        else
                        {
                            LOGGER.Log(Level.Info, "Task {0} is waiting in the ring", message.TaskId);
                            _currentWaitingList.AddOrUpdate(message.TaskId, 0, (task, value) => value);
                            _tasksInRing.AddOrUpdate(message.TaskId, 0, (task, value) => value);
                        }
                    }

                    var messages = new HashSet<RingReturnMessage>();

                    SubmitNextNodes(ref messages);

                    return messages;
                default:
                    return null;
            }
        }

        private void SubmitNextNodes(ref HashSet<RingReturnMessage> messages)
        {
            lock (_ringLock)
            {
                while (_currentWaitingList.Keys.Count > 0)
                {
                    var nextTask = _currentWaitingList.Keys.Take(1).GetEnumerator();
                    while (nextTask.MoveNext())
                    {
                        var dest = _ring.Last.Value;
                        var data = dest + ":" + Constants.AggregationRing + ":" + nextTask.Current;
                        var returnMessage = new RingReturnMessage(Utils.GetTaskNum(dest), ByteUtilities.StringToByteArrays(data));

                        messages.Add(returnMessage);
                        _ring.AddLast(nextTask.Current);
                        _currentWaitingList.TryRemove(nextTask.Current, out byte value);
                    }
                }

                if (_failureMachine.NumOfDataPoints - _failureMachine.NumOfFailedDataPoints <= _ring.Count)
                {
                    var dest = _ring.Last.Value;
                    var data = dest + ":" + Constants.AggregationRing + ":" + _ring.First.Value;
                    var returnMessage = new RingReturnMessage(Utils.GetTaskNum(dest), ByteUtilities.StringToByteArrays(data));

                    messages.Add(returnMessage);

                    LOGGER.Log(Level.Info, "Ring is closed:\n {0}->{1}", string.Join("->", _ring), _ring.First.Value);

                    lock (_waitingListLock)
                    {
                        _prevRing = _ring;
                        _ring = new LinkedList<string>();
                        _ring.AddLast(_prevRing.First.Value);
                        _currentWaitingList = _nextWaitingList;
                        _nextWaitingList = new ConcurrentDictionary<string, byte>();
                        _tasksInRing = new ConcurrentDictionary<string, byte>();
                        _tasksInRing.AddOrUpdate(_coordinatorTaskId, 0, (task, value) => value);

                        foreach (var task in _currentWaitingList.Keys)
                        {
                            _tasksInRing.AddOrUpdate(task, 0, (id, value) => value);
                        }
                    }
                }
            }

            // Continuously build the ring until there is some node waiting
            if (_currentWaitingList.Keys.Count > 0)
            {
                SubmitNextNodes(ref messages); 
            }
        }
    }
}
