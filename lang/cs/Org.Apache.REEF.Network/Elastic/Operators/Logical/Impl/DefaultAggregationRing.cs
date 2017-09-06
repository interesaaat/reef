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
using System.Text;

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
        private RingNode _currentRingHead;
        private RingNode _prevRingHead;
        private RingNode _currentRingTail;
        private StringBuilder _ringPrint;
        private RingNode _lastToken;
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

            _currentWaitingList = new HashSet<string>();
            _nextWaitingList = new HashSet<string>();
            ////_currentRing = new LinkedList<RingNode>();
            ////_prevRing = new LinkedList<RingNode>();
            _ringPrint = new StringBuilder();
            _rootTaskId = string.Empty;
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
            _currentRingHead = new RingNode(_rootTaskId);
            _currentRingTail = _currentRingHead;
            _lastToken = _currentRingHead;
            _ringPrint.Append(_rootTaskId);

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
                    if (_lastToken.Current == false)
                    {
                        var pos = _lastToken;
                        while (pos != null)
                        {
                            pos.Done = true;
                            pos.Current = false;
                            pos = pos.Next;
                        }
                        _lastToken = _currentRingHead;
                        Console.WriteLine("Token at beginnig");
                    }
                }
                else
                {
                    var head = _lastToken;

                    while (head != null && head.TaskId != taskId)
                    {
                        head.Done = true;
                        head.Current = _lastToken.Current;
                        head = head.Next;
                    }

                    if (head == null)
                    {
                        head = _currentRingHead;

                        while (head.TaskId != taskId)
                        {
                            head.Done = true;
                            head = head.Next;
                        }

                        Console.WriteLine("Token at " + taskId);
                    }
                    _lastToken = head;

                    Console.WriteLine("Token at pre" + taskId);
                    Console.WriteLine("Pre " + _lastToken.Prev.TaskId + " Next " + _lastToken.Next == null ? _lastToken.Next.TaskId : "null");
                    Console.WriteLine("Current " + _lastToken.Current);
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
                        var dest = _currentRingTail.TaskId;
                        var data = new RingMessagePayload(nextTask);
                        var returnMessage = new DriverMessage(dest, data);

                        messages.Add(returnMessage);
                        _currentRingTail.Next = new RingNode(nextTask, _currentRingTail);
                        _currentRingTail = _currentRingTail.Next;
                        _tasksInRing.Add(nextTask);
                        _currentWaitingList.Remove(nextTask);

                        _ringPrint.Append("<-" + nextTask);
                    }
                }

                if (_failureMachine.NumOfDataPoints - _failureMachine.NumOfFailedDataPoints <= _tasksInRing.Count)
                {
                    var dest = _currentRingTail.TaskId;
                    var data = new RingMessagePayload(_rootTaskId);
                    var returnMessage = new DriverMessage(dest, data);

                    messages.Add(returnMessage);
                    LOGGER.Log(Level.Info, "Ring is closed:\n {0}->{1}", _ringPrint, _rootTaskId);

                    _prevRingHead = _currentRingHead;
                    _lastToken.Current = false;
                    _currentRingHead = new RingNode(_rootTaskId);
                    _currentRingTail = _currentRingHead;
                    _currentWaitingList = _nextWaitingList;
                    _nextWaitingList = new HashSet<string>();
                    _tasksInRing = new HashSet<string> { { _rootTaskId } };

                    foreach (var task in _currentWaitingList)
                    {
                        _tasksInRing.Add(task);
                    }

                    _ringPrint = new StringBuilder(_rootTaskId);
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
                                if (reconfigureEvent.FailedTask.Id == _lastToken.TaskId)
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

    internal class RingNode
    {
        public RingNode(string taskId, RingNode prev = null)
        {
            TaskId = taskId;
            Done = false;
            Next = null;
            Prev = prev;
            Current = true;
        }

        public string TaskId { get; private set; }

        public bool Done { get; set; }

        public RingNode Next { get; set; }

        public RingNode Prev { get; set; }

        public bool Current { get; set; }

        ////public static bool operator ==(RingNode a, RingNode b)
        ////{
        ////    if (ReferenceEquals(a, b))
        ////    {
        ////        return true;
        ////    }

        ////    if (((object)a == null) || ((object)b == null))
        ////    {
        ////        return false;
        ////    }

        ////    return a.TaskId == b.TaskId;
        ////}

        ////public static bool operator !=(RingNode a, RingNode b)
        ////{
        ////    return !(a == b);
        ////}
    }
}
