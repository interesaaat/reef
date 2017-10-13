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
using System.Text;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using System.Diagnostics;

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
        private RingNode _ringHead;
        private StringBuilder _ringPrint;

        private string _rootTaskId;
        private string _taskSubscription;
        private volatile int _iteration;

        private int _rootId;
        private bool _finalized;

        private readonly Dictionary<int, DataNode> _nodes;

        private HashSet<string> _failedNodesWaiting;

        private int _availableDataPoints;

        private readonly object _lock;

        private readonly Stopwatch _timer;

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
            _failedNodesWaiting = new HashSet<string>();
            _ringPrint = new StringBuilder();
            _rootTaskId = string.Empty;
            _taskSubscription = string.Empty;
            _iteration = 1;

            _timer = Stopwatch.StartNew();
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

            lock (_lock)
            {
                if (_nodes.ContainsKey(id))
                {
                    // If the node is not reachable it is recovering.
                    // Don't add it yet to the ring otherwise we slow down closing
                    if (_finalized && _nodes[id].FailState != DataNodeState.Reachable)
                    {
                        _failedNodesWaiting.Add(taskId);
                        _nodes[id].FailState = DataNodeState.Unreachable;

                        return 0;
                    }

                    throw new ArgumentException("Task has already been added to the topology");
                }

                DataNode node = new DataNode(id, false);
                _nodes[id] = node;
                _availableDataPoints++;
            }

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

            lock (_lock)
            { 
                if (node.FailState == DataNodeState.Lost)
                {
                    return 0;
                }

                node.FailState = DataNodeState.Lost;
                _availableDataPoints--;
            }

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
            _ringHead = new RingNode(_rootTaskId, _iteration);
            _ringPrint.Append(_rootTaskId);

            _finalized = true;

            return this;
        }

        public string LogTopologyState()
        {
            lock (_lock)
            {
                return _ringPrint.ToString();
            }
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

        internal int AddTaskIdToRing(string taskId)
        {
            var addedReachableNodes = 0;

            lock (_lock)
            {
                if (_failedNodesWaiting.Contains(taskId))
                {
                    var id = Utils.GetTaskNum(taskId);

                    _availableDataPoints++;
                    _failedNodesWaiting.Remove(taskId);
                    _nodes[id].FailState = DataNodeState.Reachable;
                    addedReachableNodes++;
                }

                if (_currentWaitingList.Contains(taskId) || _tasksInRing.Contains(taskId))
                {
                    _nextWaitingList.Add(taskId);
                }
                else
                {
                    _currentWaitingList.Add(taskId);
                }
            }

            return addedReachableNodes;
        }

        internal void GetNextTasksInRing(ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                while (_currentWaitingList.Count > 0)
                {
                    var enumerator = _currentWaitingList.Take(1);
                    foreach (var nextTask in enumerator)
                    {
                        var dest = _ringHead.TaskId;
                        var data = _ringHead.Type == DriverMessageType.Ring ? (IDriverMessagePayload)new RingMessagePayload(nextTask, SubscriptionName, OperatorId) : (IDriverMessagePayload)new FailureMessagePayload(nextTask, _iteration, SubscriptionName, OperatorId);
                        var returnMessage = new ElasticDriverMessageImpl(dest, data);

                        messages.Add(returnMessage);
                        _ringHead.Next = new RingNode(nextTask, _iteration, _ringHead);
                        _ringHead.Next.Prev = _ringHead;
                        _ringHead = _ringHead.Next;
                        _tasksInRing.Add(nextTask);
                        _currentWaitingList.Remove(nextTask);
                        _ringPrint.Append(" -> " + nextTask);

                        CloseRing(ref messages);
                    }
                }
            }

            // Continuously build the ring until there is some node waiting
            if (_currentWaitingList.Count > 0)
            {
                GetNextTasksInRing(ref messages);
            }
        }

        internal void CloseRing(ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                if (_availableDataPoints <= _tasksInRing.Count)
                {
                    var dest = _ringHead.TaskId;
                    var data = _ringHead.Type == DriverMessageType.Ring ? (IDriverMessagePayload)new RingMessagePayload(_rootTaskId, SubscriptionName, OperatorId) : (IDriverMessagePayload)new FailureMessagePayload(_rootTaskId, _iteration, SubscriptionName, OperatorId);
                    var returnMessage = new ElasticDriverMessageImpl(dest, data);

                    messages.Add(returnMessage);
                    _timer.Stop();
                    LOGGER.Log(Level.Info, "Ring in Iteration {0} is closed in {1}ms with {2} nodes:\n {3} -> {4}", _iteration, _timer.ElapsedMilliseconds, _tasksInRing.Count, LogTopologyState(), _rootTaskId);

                    _iteration++;
                    _ringHead.Next = new RingNode(_rootTaskId, _iteration);
                    _ringHead.Next.Prev = _ringHead;
                    _ringHead = _ringHead.Next;
                    _currentWaitingList = _nextWaitingList;
                    _nextWaitingList = new HashSet<string>();
                    _tasksInRing = new HashSet<string> { { _rootTaskId } };

                    _ringPrint = new StringBuilder(_rootTaskId);
                    _timer.Restart();
                }
            }
        }

        internal void RetrieveTokenFromRing(string taskId, int iteration, ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                if (_ringHead.TaskId != taskId)
                {
                    var head = _ringHead;

                    while (head != null && head.TaskId != taskId)
                    {
                        head = head.Prev;
                    }

                    if (head.Iteration == iteration)
                    {
                        var dest = taskId;
                        var data = _ringHead.Type == DriverMessageType.Ring ? (IDriverMessagePayload)new RingMessagePayload(head.Next.TaskId, SubscriptionName, OperatorId) : (IDriverMessagePayload)new FailureMessagePayload(head.Next.TaskId, _iteration, SubscriptionName, OperatorId);
                        messages.Add(new ElasticDriverMessageImpl(dest, data));

                        LOGGER.Log(Level.Info, "Next token is {0}", head.Next.TaskId);
                    }
                    else
                    {
                        LOGGER.Log(Level.Info, "Request for old iteration: ignoring");
                    }
                }
            }
        }

        public IList<IElasticDriverMessage> Reconfigure(string taskId, string info)
        {
            if (taskId == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet");
            }

            List<IElasticDriverMessage> messages = new List<IElasticDriverMessage>();
            var failureInfos = info.Split(':');
            int position = int.Parse(failureInfos[0]);
            int currentIteration = int.Parse(failureInfos[1]);

            lock (_lock)
            {
                _ringPrint.Replace(taskId + " ", "X ");
                var head = _ringHead;

                while (head != null && head.TaskId != taskId)
                {
                    head = head.Prev;
                }

                if (head == null)
                {
                    LOGGER.Log(Level.Warning, "Failure in a Task never added to the ring: ignore");
                    return messages;
                }

                _tasksInRing.Remove(head.TaskId);
                _currentWaitingList.Remove(head.TaskId);
                _nextWaitingList.Remove(head.TaskId);

                var next = head.Next;
                var prev = head.Prev;

                if (next != null)
                {
                    next.Prev = head.Prev;
                }

                head.Prev.Next = next;
                head.Next = null;
                head.Prev = null;

                switch (position)
                {
                    // We are before receive, we should be ok
                    case (int)PositionTracker.Nil:
                        CloseRing(ref messages);

                        LOGGER.Log(Level.Info, "Node failed before any communication: no need to reconfigure");

                        return messages;

                    // The failure is on the node with token
                    case (int)PositionTracker.InReceive:
                    case (int)PositionTracker.AfterReceiveBeforeSend:
                        // We are at the end of the ring
                        if (next == null)
                        {
                            _ringHead = prev;
                            _ringHead.Next = null;
                            _ringHead.Type = DriverMessageType.Failure;
                            CloseRing(ref messages);
                        }
                        else
                        {
                            var data = new FailureMessagePayload(next.TaskId, next.Iteration, SubscriptionName, OperatorId);
                            var returnMessage = new ElasticDriverMessageImpl(prev.TaskId, data);
                            messages.Add(returnMessage);
                        }
                        LOGGER.Log(Level.Info, "Sending reconfiguration message: restarting from node {0}", head.TaskId);

                        return messages;

                    // The failure is on the node with token while sending
                    case (int)PositionTracker.InSend:
                    // We are after send but before a new iteration starts 
                    // Data may or may not have reached the next node
                    case (int)PositionTracker.AfterSendBeforeReceive:
                        // We are at the end of the ring
                        if (next == null)
                        {
                            _ringHead = prev;
                            _ringHead.Next = null;
                            _ringHead.Type = DriverMessageType.Failure;
                            CloseRing(ref messages);
                        }
                        else
                        {
                            next.Type = DriverMessageType.Request;

                            var data = new TokenReceivedRequest(currentIteration, SubscriptionName, OperatorId);
                            var returnMessage = new ElasticDriverMessageImpl(next.TaskId, data);
                            messages.Add(returnMessage);

                            LOGGER.Log(Level.Info, "Sending request token message to node {0} for iteration {1}", next.TaskId, currentIteration);
                        }

                        return messages;
                    default:
                        return messages;
                }
            }
        }

        public void ResumeRingFromCheckpoint(string taskId, ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                var head = _ringHead;

                while (head != null && head.TaskId != taskId && head.Type != DriverMessageType.Request)
                {
                    head = head.Prev;
                }

                // Get the last available checkpointed node
                var lastCheckpoint = head.Prev;
                head.Type = DriverMessageType.Ring;

                var data = new FailureMessagePayload(head.TaskId, head.Iteration, SubscriptionName, OperatorId);
                var returnMessage = new ElasticDriverMessageImpl(lastCheckpoint.TaskId, data);
                messages.Add(returnMessage);

                LOGGER.Log(Level.Info, "Sending reconfiguration message: restarting from node {0}", lastCheckpoint.TaskId);
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
