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
using Org.Apache.REEF.Network.Elastic.Failures;

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

        private string _rootTaskId;
        private string _taskSubscription;
        private volatile int _iteration;
        private int _rootId;
        private bool _finalized;

        private HashSet<string> _currentWaitingList;
        private HashSet<string> _nextWaitingList;
        private HashSet<string> _tasksInRing;
        private HashSet<string> _nodesWaitingToJoinRing;
        private HashSet<string> _failedNodes;
        private RingNode _ringHead;
        private readonly Dictionary<int, DataNode> _nodes;
        private readonly SortedDictionary<int, Dictionary<string, RingNode>> _ringNodes;

        private volatile int _availableDataPoints;

        private readonly object _lock;
        private readonly Stopwatch _timer;
        private long _totTime;
        private int _totNumberofNodes;

        public RingTopology(int rootId)
        {
            _rootId = rootId;
            _finalized = false;
            _availableDataPoints = 0;
            OperatorId = -1;
            SubscriptionName = string.Empty;

            _nodes = new Dictionary<int, DataNode>();
            _ringNodes = new SortedDictionary<int, Dictionary<string, RingNode>>();
            _ringNodes.Add(1, new Dictionary<string, RingNode>());
            _currentWaitingList = new HashSet<string>();
            _nextWaitingList = new HashSet<string>();
            _nodesWaitingToJoinRing = new HashSet<string>();
            _failedNodes = new HashSet<string>();
            _rootTaskId = string.Empty;
            _taskSubscription = string.Empty;
            _iteration = 1;

            _timer = Stopwatch.StartNew();
            _totTime = 0;
            _totNumberofNodes = 0;
            _lock = new object();
        }

        public int OperatorId { get; set; }

        public string SubscriptionName { get; set; }

        public bool AddTask(string taskId, ref IFailureStateMachine failureMachine)
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
                        if (!_failedNodes.Contains(taskId))
                        {
                            throw new IllegalStateException("Resuming task never failed: Aborting");
                        }

                        _failedNodes.Remove(taskId);
                        _nodesWaitingToJoinRing.Add(taskId);
                        _nodes[id].FailState = DataNodeState.Unreachable;
                        failureMachine.AddDataPoints(0, false);
                        return false;
                    }

                    throw new ArgumentException("Task has already been added to the topology");
                }

                DataNode node = new DataNode(id, false);
                _nodes[id] = node;

                if (_finalized)
                {
                    // New node but elastically added. It should be gracefully added to the ring.
                    _nodesWaitingToJoinRing.Add(taskId);
                    _nodes[id].FailState = DataNodeState.Unreachable;
                    failureMachine.AddDataPoints(1, true);
                    failureMachine.RemoveDataPoints(1);
                    return false;
                }
                else
                {
                    _availableDataPoints++;

                    // This is required later in order to build the topology
                    if (_taskSubscription == string.Empty)
                    {
                        _taskSubscription = Utils.GetTaskSubscriptions(taskId);
                    }
                }
            }

            failureMachine.AddDataPoints(1, true);
            return true;
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
                LOGGER.Log(Level.Info, string.Format("{0} is not part of this topology: ignoring", taskId));
            }

            DataNode node = _nodes[id];

            lock (_lock)
            {
                var state = node.FailState;

                _nodesWaitingToJoinRing.Remove(taskId);
                _failedNodes.Add(taskId);
                node.FailState = DataNodeState.Lost;

                if (state != DataNodeState.Reachable)
                {
                    LOGGER.Log(Level.Info, string.Format("Removing {0} that was already in state {1}", taskId, state));
                    return 0;
                }

                _availableDataPoints--;
                _tasksInRing.Remove(taskId);
                _currentWaitingList.Remove(taskId);
                _nextWaitingList.Remove(taskId);
            }

            return 1;
        }

        public bool CanBeScheduled()
        {
            return _nodes.ContainsKey(_rootId);
        }

        public ITopology Build()
        {
            if (_finalized)
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

            _finalized = true;

            return this;
        }

        public string LogTopologyState()
        {
            lock (_lock)
            {
                var str = new StringBuilder();
                var node = _ringHead;
                int count = 1;

                if (node != null)
                {
                    str.Append(node.TaskId + "-" + node.Iteration);
                    node = node.Prev;
                }

                while (node != null && count < _availableDataPoints)
                {
                    str.Append(" <- " + node.TaskId + "-" + node.Iteration);
                    node = node.Prev;
                    count++;
                }
                return str.ToString();
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

        public IList<IElasticDriverMessage> Reconfigure(string taskId, string info)
        {
            if (taskId == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet");
            }

            List<IElasticDriverMessage> messages = new List<IElasticDriverMessage>();

            lock (_lock)
            {
                if (info != string.Empty)
                {
                    var failureInfos = info.Split(':');
                    int position = int.Parse(failureInfos[0]);
                    int iteration = int.Parse(failureInfos[1]);

                    if (_ringNodes.TryGetValue(iteration, out Dictionary<string, RingNode> nodes))
                    {
                        nodes.TryGetValue(taskId, out RingNode node);

                        switch (position)
                        {
                            // We are before receive, we should be ok
                            case (int)PositionTracker.Nil:
                                LOGGER.Log(Level.Info, "Node failed before any communication: no need to reconfigure");
                                break;

                            // The failure is on the node with token
                            case (int)PositionTracker.InReceive:
                            case (int)PositionTracker.AfterReceiveBeforeSend:
                                if (node == null)
                                {
                                    if (!_ringNodes[iteration].TryGetValue(taskId, out node))
                                    {
                                        LOGGER.Log(Level.Info, "Node failed while waiting for message: ignore");
                                        break;
                                    }
                                }

                                var next = node.Next;
                                var prev = node.Prev;

                                // We are at the end of the ring
                                if (next == null)
                                {
                                    _ringHead = prev;
                                    _ringHead.Type = DriverMessageType.Failure;
                                }
                                else
                                {
                                    var data = new FailureMessagePayload(next.TaskId, next.Iteration, SubscriptionName, OperatorId);
                                    var returnMessage = new ElasticDriverMessageImpl(prev.TaskId, data);
                                    messages.Add(returnMessage);
                                }
                                LOGGER.Log(Level.Info, "Sending reconfiguration message: restarting from node {0}", node.TaskId);
                                break;

                            // The failure is on the node with token while sending
                            case (int)PositionTracker.InSend:
                            // We are after send but before a new iteration starts 
                            // Data may or may not have reached the next node
                            case (int)PositionTracker.AfterSendBeforeReceive:
                                if (node == null)
                                {
                                    if (!_ringNodes[iteration].TryGetValue(taskId, out node))
                                    {
                                        throw new IllegalStateException(string.Format("Failure in {0} in iteration {1} not recognized: current ring is in {2}", taskId, iteration, _iteration));
                                    }
                                }

                                next = node.Next;
                                prev = node.Prev;

                                // We are at the end of the ring
                                if (next == null)
                                {
                                    _ringHead = prev;
                                    _ringHead.Type = DriverMessageType.Failure;
                                }
                                else
                                {
                                    next.Type = DriverMessageType.Request;

                                    var data = new TokenReceivedRequest(iteration, SubscriptionName, OperatorId);
                                    var returnMessage = new ElasticDriverMessageImpl(next.TaskId, data);
                                    messages.Add(returnMessage);

                                    LOGGER.Log(Level.Info, "Sending request token message to node {0} for iteration {1}", next.TaskId, iteration);
                                }
                                break;
                            default:
                                break;
                        }
                    }
                }

                RemoveTaskFromRing(taskId);
            }

            CloseRing(ref messages);

            return messages;
        }

        internal void RemoveTaskFromRing(string taskId)
        {
            foreach (var dict in _ringNodes.Values)
            {
                if (dict.TryGetValue(taskId, out RingNode node))
                {
                    if (node.Next != null)
                    {
                        node.Next.Prev = node.Prev;
                    }
                    if (node.Prev != null)
                    {
                        node.Prev.Next = node.Next;
                    }

                    node = null;
                    dict.Remove(taskId);
                }
            }
        }

        internal void AddTaskToRing(string taskId, int iteration, ref List<IElasticDriverMessage> messages, ref IFailureStateMachine failureStateMachine)
        {
            var addedReachableNodes = 0;

            lock (_lock)
            {
                if (_failedNodes.Contains(taskId))
                {
                    LOGGER.Log(Level.Warning, "Trying to add to the ring a failed task: ignoring");
                    return;
                }

                if (_nodesWaitingToJoinRing.Contains(taskId))
                {
                    var id = Utils.GetTaskNum(taskId);

                    _availableDataPoints++;
                    _nodesWaitingToJoinRing.Remove(taskId);
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

            failureStateMachine.AddDataPoints(addedReachableNodes, false);
        }

        internal void GetNextTasksInRing(ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                while (_currentWaitingList.Count > 0)
                {
                    var nextTask = _currentWaitingList.First();
                    var dest = _ringHead.TaskId;
                    var data = _ringHead.Type == DriverMessageType.Ring ? (DriverMessagePayload)new RingMessagePayload(nextTask, SubscriptionName, OperatorId, _iteration) : (DriverMessagePayload)new FailureMessagePayload(nextTask, _iteration, SubscriptionName, OperatorId);
                    var returnMessage = new ElasticDriverMessageImpl(dest, data);

                    messages.Add(returnMessage);
                    LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2}", dest, nextTask, _iteration);

                    _ringHead.Next = new RingNode(nextTask, _iteration, _ringHead);
                    _ringHead = _ringHead.Next;
                    _tasksInRing.Add(nextTask);
                    _currentWaitingList.Remove(nextTask);
                    _ringNodes[_iteration].Add(nextTask, _ringHead);
                }

                CloseRing(ref messages);
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
                    var data = _ringHead.Type == DriverMessageType.Ring ? (DriverMessagePayload)new RingMessagePayload(_rootTaskId, SubscriptionName, OperatorId, _iteration) : (DriverMessagePayload)new FailureMessagePayload(_rootTaskId, _iteration, SubscriptionName, OperatorId);
                    var returnMessage = new ElasticDriverMessageImpl(dest, data);

                    LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2}", dest, _rootTaskId, _iteration);
                    messages.Add(returnMessage);

                    _timer.Stop();
                    _totTime += _timer.ElapsedMilliseconds;
                    _totNumberofNodes += _tasksInRing.Count;
                    LOGGER.Log(Level.Info, string.Format("Ring in Iteration {0} is closed in {1}ms with {2} nodes", _iteration, _timer.ElapsedMilliseconds, _tasksInRing.Count, (float)_totTime / 1000.0, _totTime / _iteration));

                    _ringHead.Next = new RingNode(_rootTaskId, _iteration, _ringHead);
                    _ringHead = _ringHead.Next;
                    _currentWaitingList = _nextWaitingList;
                    _nextWaitingList = new HashSet<string>();
                    _tasksInRing = new HashSet<string> { { _rootTaskId } };
                    _ringNodes[_iteration].Add(_rootTaskId, _ringHead);

                    _iteration++;
                    _ringNodes.Add(_iteration, new Dictionary<string, RingNode>());
                    CleanPreviousRings();

                    _timer.Restart();
                }
            }
        }

        internal void RetrieveTokenFromRing(string taskId, int iteration, ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                if (!_ringNodes.ContainsKey(iteration))
                {
                    LOGGER.Log(Level.Warning, string.Format("Iteration {0} not found", iteration));
                    return;
                }

                if (!_ringNodes[iteration].TryGetValue(taskId, out RingNode node))
                {
                    var msg = "Node not found: ";
                    if (_currentWaitingList.Count > 0 || _nodesWaitingToJoinRing.Count > 0)
                    {
                        LOGGER.Log(Level.Warning, msg + "going to flush nodes in queue");

                        GetNextTasksInRing(ref messages);
                    }
                    else
                    {
                        LOGGER.Log(Level.Warning, msg + " waiting");
                    }

                    return;
                }

                if (node.Next != null)
                {
                    var dest = taskId;
                    var data = node.Type == DriverMessageType.Ring ? (DriverMessagePayload)new RingMessagePayload(node.Next.TaskId, SubscriptionName, OperatorId, node.Next.Iteration) : (DriverMessagePayload)new FailureMessagePayload(node.Next.TaskId, node.Next.Iteration, SubscriptionName, OperatorId);
                    messages.Add(new ElasticDriverMessageImpl(dest, data));
                    LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2} in retrieve", dest, node.Next.TaskId, node.Next.Iteration);
                } 
            }
        }

        internal void RetrieveMissingDataFromRing(ref List<IElasticDriverMessage> messages)
        {
            Dictionary<string, RingNode> dict = null;
            var iteration = _iteration;
            lock (_lock)
            {
                dict = _ringNodes[iteration];

                // The ring may have just started a new iteration where no node has joined yet.
                // In this care retrieve the iteration - 1
                if (dict.Count == 0 && iteration > 0)
                {
                    dict = _ringNodes[--iteration];
                }
            }

            if (dict.Count > 0)
            {
                var head = dict.First().Value;

                while (head.Next != null)
                {
                    head = head.Next;
                }

                RetrieveMissingDataFromRing(head.TaskId, iteration, ref messages);
            }
            else
            {
                throw new IllegalStateException("Trying to recover an empty ring: failing");
            } 
        }

        internal void RetrieveMissingDataFromRing(string taskId, int iteration, ref List<IElasticDriverMessage> returnMessages)
        {
            lock (_lock)
            {
                RingNode node;
                if (_ringNodes[iteration].TryGetValue(taskId, out node))
                {
                    var dest = node.Prev.TaskId;
                    var data = new ResumeMessagePayload(node.TaskId, node.Iteration, SubscriptionName, OperatorId);
                    returnMessages.Add(new ElasticDriverMessageImpl(dest, data));
                    LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2}", dest, node.TaskId, node.Iteration);
                }
                else
                {
                    LOGGER.Log(Level.Warning, "Impossible to retrieve missing data for Task {0} in iteration {1}", taskId, iteration);
                }
            }
        }

        internal void ResumeRingFromCheckpoint(string taskId, int iteration, ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                if (_ringNodes[iteration].TryGetValue(taskId, out RingNode node) && node.Prev != null)
                {
                    var data = new FailureMessagePayload(node.TaskId, node.Iteration, SubscriptionName, OperatorId);
                    var returnMessage = new ElasticDriverMessageImpl(node.Prev.TaskId, data);

                    node.Type = DriverMessageType.Ring;
                    messages.Add(returnMessage);
                    LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2}", node.Prev.TaskId, node.TaskId, node.Iteration);

                    LOGGER.Log(Level.Info, "Resuming ring from node {0}", node.Prev.TaskId);
                }
                else
                {
                    LOGGER.Log(Level.Warning, "Impossible to resume from checkpoint for Task {0} in iteration {1}: Ignoring", taskId, iteration);
                }
            }
        }

        internal string Statistics()
        {
            return string.Format("Total ring computation time {0}s\nAverage ring computation time {1}ms\nAverage number of nodes in ring {2}", (float)_totTime / 1000.0, _totTime / (_iteration - 1), (float)_totNumberofNodes / (_iteration - 1));
        }

        private void CleanPreviousRings()
        {
            lock (_lock)
            {
                if (_ringNodes.Count > 3)
                {
                    var smallerDict = _ringNodes.First();
                    var keys = smallerDict.Value.Keys.ToArray();

                    for (int i = 0; i < keys.Length; i++)
                    {
                        var task = keys[i];
                        smallerDict.Value[task].Prev = null;
                        smallerDict.Value[task].Next = null;
                        smallerDict.Value[task] = null;
                    }

                    _ringNodes.Remove(smallerDict.Key);
                }
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
