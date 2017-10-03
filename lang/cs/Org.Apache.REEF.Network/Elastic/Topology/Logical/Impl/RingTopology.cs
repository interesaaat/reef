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

using System;
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Globalization;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Driver;
using System.Text;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    /// <summary>
    /// Ring topology for aggregation ring operator.
    /// At configuration time the topology initialize as full connected.
    /// At run-time a ring topology is generated dynamically as nodes join the ring. 
    /// </summary>
    public class RingTopology : ITopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(RingTopology));

        private HashSet<string> _currentWaitingList;
        private HashSet<string> _nextWaitingList;
        private HashSet<string> _tasksInRing;

        // Now I only use current and prev. Eventually one could play with this plus checkpointing,
        // for instance if we checkpoint for up to 5 iterations, we can have an array of prev
        private RingNode _currentRingHead;
        private RingNode _prevRingHead;
        private RingNode _currentRingTail;
        private RingNode _prevRingTail;
        private StringBuilder _ringPrint;

        private string _rootTaskId;
        private string _taskSubscription;
        private volatile int _iteration;

        private int _rootId;
        private bool _finalized;

        private readonly Dictionary<int, DataNode> _nodes;

        private int _availableDataPoints;

        private readonly object _lock;

        public RingTopology(int rootId)
        {
            _rootId = rootId;
            _finalized = false;
            _availableDataPoints = 0;
            OperatorId = -1;
            SubscriptionName = string.Empty;

            _nodes = new Dictionary<int, DataNode>();

            _currentWaitingList = new HashSet<string>();
            _nextWaitingList = new HashSet<string>();
            _ringPrint = new StringBuilder();
            _rootTaskId = string.Empty;
            _taskSubscription = string.Empty;
            _iteration = 1;

            _lock = new object();
        }

        public int OperatorId { get; set; }

        public string SubscriptionName { get; set; }

        public int AddTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            var id = Utils.GetTaskNum(taskId);
            
            if (_nodes.ContainsKey(id))
            {
                if (_finalized && _nodes[id].FailState != DataNodeState.Reachable)
                {
                    _nodes[id].FailState = DataNodeState.Reachable;
                    _availableDataPoints++;

                    return 1;
                }

                throw new ArgumentException("Task has already been added to the topology");
            }

            DataNode node = new DataNode(id, false);
            _nodes[id] = node;
            _availableDataPoints++;

            // This is required later in order to build the topology
            if (_taskSubscription == string.Empty)
            {
                _taskSubscription = Utils.GetTaskSubscriptions(taskId);
            }

            return 1;
        }

        public int RemoveTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            var id = Utils.GetTaskNum(taskId);

            if (!_nodes.ContainsKey(id))
            {
                throw new ArgumentException("Task is not part of this topology");
            }

            DataNode node = _nodes[id];

            if (node.FailState == DataNodeState.Lost)
            {
                return 0;
            }

            node.FailState = DataNodeState.Lost;
            _availableDataPoints--;

            return 1;
        }

        public ITopology Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Topology cannot be built more than once");
            }

            if (!_nodes.ContainsKey(_rootId))
            {
                throw new IllegalStateException("Topology cannot be built because the root node is missing");
            }

            if (OperatorId <= 0)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any operator");
            }

            if (SubscriptionName == string.Empty)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any subscription");
            }

            _rootTaskId = Utils.BuildTaskId(_taskSubscription, _rootId);
            _tasksInRing = new HashSet<string> { { _rootTaskId } };
            _currentRingHead = new RingNode(_rootTaskId, _iteration);
            _currentRingTail = _currentRingHead;
            _ringPrint.Append(_rootTaskId);

            _finalized = true;

            return this;
        }

        public string LogTopologyState()
        {
            return _ringPrint.ToString();
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            foreach (var tId in _nodes.Values)
            {
                if (tId.TaskId != taskId)
                {
                    confBuilder.BindSetEntry<GroupCommunicationConfigurationOptions.TopologyChildTaskIds, int>(
                        GenericType<GroupCommunicationConfigurationOptions.TopologyChildTaskIds>.Class,
                        tId.TaskId.ToString(CultureInfo.InvariantCulture));
                }
            }
            confBuilder.BindNamedParameter<GroupCommunicationConfigurationOptions.TopologyRootTaskId, int>(
                GenericType<GroupCommunicationConfigurationOptions.TopologyRootTaskId>.Class,
                _rootId.ToString(CultureInfo.InvariantCulture));
        }

        internal void AddTaskIdToRing(string taskId)
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

        internal IList<ElasticDriverMessageImpl> GetNextTasksInRing()
        {
            IList<ElasticDriverMessageImpl> messages = new List<ElasticDriverMessageImpl>();

            SubmitNextNodes(ref messages);

            return messages;
        }

        internal void SubmitNextNodes(ref IList<ElasticDriverMessageImpl> messages)
        {
            lock (_lock)
            {
                while (_currentWaitingList.Count > 0)
                {
                    var enumerator = _currentWaitingList.Take(1);
                    foreach (var nextTask in enumerator)
                    {
                        var dest = _currentRingTail.TaskId;
                        var data = _currentRingTail.Type == DriverMessageType.Ring ? (IDriverMessagePayload)new RingMessagePayload(nextTask, SubscriptionName, OperatorId) : (IDriverMessagePayload)new FailureMessagePayload(nextTask, _currentRingTail.Iteration, SubscriptionName, OperatorId);
                        var returnMessage = new ElasticDriverMessageImpl(dest, data);

                        messages.Add(returnMessage);
                        _currentRingTail.Next = new RingNode(nextTask, _iteration, _currentRingTail);
                        _currentRingTail = _currentRingTail.Next;
                        _tasksInRing.Add(nextTask);
                        _currentWaitingList.Remove(nextTask);

                        _ringPrint.Append("->" + nextTask);
                    }
                }

                if (_availableDataPoints <= _tasksInRing.Count)
                {
                    var dest = _currentRingTail.TaskId;
                    var data = _currentRingTail.Type == DriverMessageType.Ring ? (IDriverMessagePayload)new RingMessagePayload(_rootTaskId, SubscriptionName, OperatorId) : (IDriverMessagePayload)new FailureMessagePayload(_rootTaskId, _currentRingTail.Iteration, SubscriptionName, OperatorId);
                    var returnMessage = new ElasticDriverMessageImpl(dest, data);

                    messages.Add(returnMessage);
                    LOGGER.Log(Level.Info, "Ring in Iteration {0} is closed:\n {1}->{2}", _iteration, LogTopologyState(), _rootTaskId);

                    _prevRingHead = _currentRingHead;
                    _prevRingTail = _currentRingTail;
                    _iteration++;
                    _currentRingHead = new RingNode(_rootTaskId, _iteration);
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

        public List<IElasticDriverMessage> Reconfigure(string taskId, string info)
        {
            if (taskId == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet");
            }

            var messages = new List<IElasticDriverMessage>();
            var failureInfos = info.Split(':');
            int position = int.Parse(failureInfos[0]);
            int currentIteration = int.Parse(failureInfos[1]);

            _ringPrint.Replace(taskId, "X");

            switch (position)
            {
                // We are before receive, we should be ok
                case (int)PositionTracker.Nil:
                    LOGGER.Log(Level.Info, "Node failed before any communication: no need to reconfigure");
                    return messages;

                // The failure is on the node with token
                case (int)PositionTracker.InReceive:
                case (int)PositionTracker.AfterReceiveBeforeSend:
                    lock (_lock)
                    {
                        var head = _prevRingHead ?? _currentRingHead;

                        // Position at the right ring
                        if (head.Iteration < currentIteration)
                        {
                            head = _currentRingHead;
                        }

                        while (head != null && head.TaskId != taskId)
                        {
                            head = head.Next;
                        }

                        if (head == null)
                        {
                            Console.WriteLine("Ops");
                            Console.WriteLine(currentIteration);
                            Console.WriteLine(_iteration);
                        }

                        _tasksInRing.Remove(head.TaskId);
                        _currentWaitingList.Remove(head.TaskId);
                        _nextWaitingList.Remove(head.TaskId);

                        // Get the last available checkpointed node
                        var lastCheckpoint = head.Prev;
                        var nextNode = head.Next;
                        head.Prev = null;

                        // We are at the end of the ring
                        if (nextNode == null)
                        {
                            // We are on the current ring
                            if (head.Iteration == _iteration)
                            {
                                _currentRingTail = lastCheckpoint;
                                _currentRingTail.Next = null;
                                _currentRingTail.Type = DriverMessageType.Failure;
                                head = _currentRingTail;
                            }
                            else
                            {
                                // We are on the prev ring
                                _prevRingTail = lastCheckpoint;
                                _prevRingTail.Next = null;
                                _prevRingTail.Type = DriverMessageType.Failure;
                                head = _prevRingTail;

                                var data = new FailureMessagePayload(_currentRingHead.TaskId, _currentRingHead.Iteration, SubscriptionName, OperatorId);
                                var returnMessage = new ElasticDriverMessageImpl(head.TaskId, data);
                                messages.Add(returnMessage);
                            }
                        }
                        else
                        {
                            _tasksInRing.Remove(head.TaskId);
                            _currentWaitingList.Remove(head.TaskId);
                            _nextWaitingList.Remove(head.TaskId);
                            head.Next = null;
                            head = lastCheckpoint;
                            head.Next = nextNode;
                            nextNode.Prev = head;

                            var data = new FailureMessagePayload(nextNode.TaskId, nextNode.Iteration, SubscriptionName, OperatorId);
                            var returnMessage = new ElasticDriverMessageImpl(head.TaskId, data);
                            messages.Add(returnMessage);
                        }
                        LOGGER.Log(Level.Info, "Sending reconfiguration message: restarting from node {0}", head.TaskId);
                    }
                    return messages;

                // The failure is on the node with token while sending
                case (int)PositionTracker.InSend:
                    // Need to check if successive node received message or not
                    return messages;

                // We are after send but before a new iteration starts
                case (int)PositionTracker.AfterSendBeforeReceive:
                    LOGGER.Log(Level.Info, "Node failed after communicating: no need to reconfigure");
                    return messages;
                default:
                    return messages;
            }
        }
    }

    internal class RingNode
    {
        public RingNode(string taskId, int iteration, RingNode prev = null)
        {
            TaskId = taskId;
            Iteration = iteration;
            Next = null;
            Prev = prev;
            Type = DriverMessageType.Ring;
        }

        public string TaskId { get; private set; }

        public int Iteration { get; set; }

        public RingNode Next { get; set; }

        public RingNode Prev { get; set; }

        public DriverMessageType Type { get; set; }
    }
}
