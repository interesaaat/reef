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
using Org.Apache.REEF.Network.Elastic.Failures.Impl;

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
        private LinkedList<string> _currentRing;
        private LinkedList<string> _prevRing;
        private LinkedList<string> _ring;
        private string _lastToken;
        private string _rootTaskId;
        private string _taskSubscription;

        private readonly object _lock;

        public DefaultAggregationRing(
            int coordinatorId,
            ElasticOperator prev,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null,
                prev,
                new RingTopology(coordinatorId),
                failureMachine,
                checkpointLevel,
                configurations)
        {
            MasterId = coordinatorId;
            OperatorName = Constants.AggregationRing;
            _rootTaskId = string.Empty;

            _currentWaitingList = new HashSet<string>();
            _nextWaitingList = new HashSet<string>();
            _currentRing = new LinkedList<string>();
            _prevRing = new LinkedList<string>();
            _ring = new LinkedList<string>();
            _lastToken = string.Empty;
            _taskSubscription = string.Empty;

            _lock = new object();
        }

        public override bool AddTask(string taskId)
        {
            // This is required later in order to build the topology
            if (_taskSubscription == string.Empty)
            {
                _taskSubscription = Utils.GetTaskSubscriptions(taskId);
            }

            return base.AddTask(taskId);
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder.BindImplementation(GenericType<IElasticBasicOperator<T>>.Class, GenericType<Physical.Impl.DefaultAggregationRing<T>>.Class);
            SetMessageType(typeof(Physical.Impl.DefaultAggregationRing<T>), ref confBuilder);
        }

        public override ElasticOperator BuildState()
        {
            _rootTaskId = Utils.BuildTaskId(_taskSubscription, MasterId);
            _tasksInRing = new HashSet<string> { { _rootTaskId } };
            _currentRing.AddLast(_rootTaskId);
            _lastToken = _rootTaskId;
            _ring.AddLast(_rootTaskId);

            return base.BuildState();
        }

        protected override ISet<DriverMessage> ReactOnTaskMessage(ITaskMessage message)
        {
            var msgReceived = (RingTaskMessageType)BitConverter.ToUInt16(message.Message, 0);
            var ring = _topology as RingTopology;

            switch (msgReceived)
            {
                case RingTaskMessageType.JoinTheRing:

                    AddTaskIdToRing(message.TaskId);

                    return GetNextTasksInRing();
                case RingTaskMessageType.TokenReceived:
                    UpdateTokenPosition(message.TaskId);
                    return new HashSet<DriverMessage>();
                default:
                    return null;
            }
        }

        private void AddTaskIdToRing(string taskId)
        {
            lock (_lock)
            {
                if (_currentWaitingList.Contains(taskId) || _tasksInRing.Contains(taskId))
                {
                    _nextWaitingList.Add(taskId);
                }
                else
                {
                    _currentWaitingList.Add(taskId);
                }
            }
        }

        private ISet<DriverMessage> GetNextTasksInRing()
        {
            var messages = new HashSet<DriverMessage>();

            SubmitNextNodes(ref messages);

            return messages;
        }

        private void UpdateTokenPosition(string taskId)
        {
            lock (_lock)
            {
                // The taskId can be:
                // 1) in tasksInRing, in which case we are working on the current ring;
                // 2) in prevRing if we are constructing the ring of the successvie iteration;
                // 3) if it is neither in tasksInRing nor in prevRing it must be a late token message therefore we can ignore it.
                if (taskId == _rootTaskId)
                {
                    // We are at the end of previous ring
                    if (_prevRing.Count > 0 && _lastToken == _prevRing.First.Value)
                    {
                        _lastToken = taskId;
                        _prevRing.Clear();
                    }
                }
                else if (_tasksInRing.Contains(taskId))
                {
                    if (_currentRing.Contains(taskId))
                    {
                        if (_prevRing.Count > 0 && _lastToken == _prevRing.First.Value)
                        {
                            _prevRing.Clear();
                        }

                        var head = _currentRing.First;

                        while (head.Value != taskId)
                        {
                            head = head.Next;
                            _currentRing.RemoveFirst();
                        }
                        _lastToken = taskId;
                    }
                }
                else if (_prevRing.Contains(taskId))
                {
                    var head = _prevRing.First;

                    while (head.Value != taskId)
                    {
                        head = head.Next;
                        _prevRing.RemoveFirst();
                    }
                    _lastToken = taskId;
                }
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
                        var dest = _currentRing.Last.Value;
                        var data = new RingMessagePayload(nextTask);
                        var returnMessage = new DriverMessage(dest, data);

                        messages.Add(returnMessage);
                        _currentRing.AddLast(nextTask);
                        _tasksInRing.Add(nextTask);
                        _currentWaitingList.Remove(nextTask);

                        _ring.AddLast(nextTask);
                    }
                }

                if (_failureMachine.NumOfDataPoints - _failureMachine.NumOfFailedDataPoints <= _tasksInRing.Count)
                {
                    var dest = _currentRing.Last.Value;
                    var data = new RingMessagePayload(_rootTaskId);
                    var returnMessage = new DriverMessage(dest, data);

                    messages.Add(returnMessage);
                    LOGGER.Log(Level.Info, "Ring is closed:\n {0}->{1}", string.Join("->",_ring), _rootTaskId);

                    _prevRing = _currentRing;
                    _currentRing = new LinkedList<string>();
                    _currentRing.AddLast(_rootTaskId);
                    _currentWaitingList = _nextWaitingList;
                    _nextWaitingList = new HashSet<string>();
                    _tasksInRing = new HashSet<string> { { _rootTaskId } };

                    foreach (var task in _currentWaitingList)
                    {
                        _tasksInRing.Add(task);
                    }

                    _ring = new LinkedList<string>();
                    _ring.AddLast(_rootTaskId);
                }
            }

            // Continuously build the ring until there is some node waiting
            if (_currentWaitingList.Count > 0)
            {
                SubmitNextNodes(ref messages);
            }
        }

        public new void OnReconfigure(IReconfigure reconfigureEvent)
        {
            var ring = _topology as RingTopology;

            if (reconfigureEvent.FailedTask.Id == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet");
            }

            if (_checkpointLevel > CheckpointLevel.None)
            {
                if (reconfigureEvent.FailedTask.AsError() is OperatorException)
                {
                    var exception = reconfigureEvent.FailedTask.AsError() as OperatorException;
                    if (exception.OperatorId == _id)
                    {
                        switch (int.Parse(exception.AdditionalInfo))
                        {
                            // The failure is on the node with token
                            case (int)PositionTracker.AfterReceiveBeforeSend:
                                if (reconfigureEvent.FailedTask.Id == _lastToken)
                                {
                                    // Get the last available checkpointed node
                                    var diff = _tasksInRing;
                                    diff.ExceptWith(_currentWaitingList);
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    else
                    {
                        ////reconfigureEvent.ReconfigureOperator = false;
                        throw new NotImplementedException("Future work");
                    }
                }
            }
        }
    }
}
