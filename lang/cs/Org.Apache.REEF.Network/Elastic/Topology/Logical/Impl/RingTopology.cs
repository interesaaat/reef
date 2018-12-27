﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using System.Diagnostics;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Enum;

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
        private string _taskStage;
        private volatile int _iteration;
        private int _rootId;
        private bool _finalized;

        private HashSet<string> _currentWaitingList;
        private HashSet<string> _nextWaitingList;
        private HashSet<string> _tasksInRing;
        private HashSet<string> _nodesWaitingToJoinRing;
        private RingNode _ringHead;
        private readonly Dictionary<int, DataNode> _nodes;
        private readonly SortedDictionary<int, RingNode> _prevRingHeads;

        private volatile int _availableDataPoints;
        private int _totNumberofNodes;

        private readonly object _lock;

        public RingTopology(int rootId)
        {
            _rootId = rootId;
            _finalized = false;
            _availableDataPoints = 0;
            OperatorId = -1;
            StageName = string.Empty;

            _nodes = new Dictionary<int, DataNode>();
            _prevRingHeads = new SortedDictionary<int, RingNode>();
            _currentWaitingList = new HashSet<string>();
            _nextWaitingList = new HashSet<string>();
            _nodesWaitingToJoinRing = new HashSet<string>();
            _rootTaskId = string.Empty;
            _taskStage = string.Empty;
            _iteration = 1;

            _totNumberofNodes = 0;
            _lock = new object();
        }

        public int OperatorId { get; set; }

        public string StageName { get; set; }

        public bool AddTask(string taskId, IFailureStateMachine failureMachine)
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
                    if (_taskStage == string.Empty)
                    {
                        _taskStage = Utils.GetTaskStages(taskId);
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
                throw new ArgumentException("Task is not part of this topology");
            }

            DataNode node = _nodes[id];

            lock (_lock)
            {
                var state = node.FailState;

                _nodesWaitingToJoinRing.Remove(taskId);
                node.FailState = DataNodeState.Lost;

                if (state != DataNodeState.Reachable)
                {
                    LOGGER.Log(Level.Warning, string.Format("Removing {0} that was already in state {1}", taskId, state));
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

            if (StageName == string.Empty)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any stage");
            }

            _rootTaskId = Utils.BuildTaskId(_taskStage, _rootId);
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
                RingNode prev = null;
                int count = 1;

                if (node != null)
                {
                    str.Append(node.TaskId + "-" + node.Iteration);
                    prev = node;
                    node = node.Prev;
                }

                while (node != null && count < _availableDataPoints)
                {
                    str.Append(" <- " + node.TaskId + "-" + node.Iteration);
                    prev = node;
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
                    confBuilder.BindSetEntry<Config.OperatorParameters.TopologyChildTaskIds, int>(
                        GenericType<Config.OperatorParameters.TopologyChildTaskIds>.Class,
                        tId.TaskId.ToString(CultureInfo.InvariantCulture));
                }
            }
            confBuilder.BindNamedParameter<Config.OperatorParameters.TopologyRootTaskId, int>(
                GenericType<Config.OperatorParameters.TopologyRootTaskId>.Class,
                _rootId.ToString(CultureInfo.InvariantCulture));
        }

        public IList<IElasticDriverMessage> Reconfigure(string taskId, Optional<string> info, Optional<int> iteration)
        {
            if (taskId == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet");
            }

            List<IElasticDriverMessage> messages = new List<IElasticDriverMessage>();

            lock (_lock)
            {   
                if (_ringHead.TaskId == taskId)
                {
                    LOGGER.Log(Level.Info, "Node failed is head of ring: need to reconfigure");

                    // We remove the failed node from the head and resume computation
                    var newHead = _ringHead.Prev;
                    _ringHead.Prev = null;
                    _ringHead = newHead;

                    TopologyUpdateResponse(_ringHead.TaskId, ref messages, Optional<IFailureStateMachine>.Empty());
                }

                RemoveTaskFromRing(taskId);
            }

            return messages;
        }

        internal void RemoveTaskFromRing(string taskId)
        {
            lock (_lock)
            {
                // If is head, set head to the prev node
                if (_ringHead.TaskId == taskId)
                {
                    var newHead = _ringHead.Prev;
                    _ringHead.Prev = null;
                    _ringHead = newHead;
                }

                var node = _ringHead;

                // Look for all occurrences of taskId in the ring
                while (node != null)
                {
                    if (node.TaskId == taskId)
                    {
                        if (node.Next != null)
                        {
                            node.Next.Prev = node.Prev;
                        }
                        if (node.Prev != null)
                        {
                            node.Prev.Next = node.Next;
                        }
                    }
                }
            }
        }

        internal void AddTaskToRing(string taskId, IFailureStateMachine failureStateMachine)
        {
            var addedReachableNodes = 0;

            lock (_lock)
            {
                var id = Utils.GetTaskNum(taskId);

                if (_nodes[id].FailState == DataNodeState.Lost)
                {
                    LOGGER.Log(Level.Warning, "Trying to add to the ring a failed task: ignoring");
                    return;
                }

                if (_nodesWaitingToJoinRing.Contains(taskId))
                {
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

        public void TopologyUpdateResponse(string taskId, ref List<IElasticDriverMessage> messages, Optional<IFailureStateMachine> failureStateMachine)
        {
            lock (_lock)
            {
                if (taskId == _ringHead.TaskId)
                {
                    if (_availableDataPoints <= _tasksInRing.Count)
                    {
                        var dest = _ringHead.TaskId;
                        var data = new RingMessagePayload(_rootTaskId, StageName, OperatorId, _iteration);
                        var returnMessage = new ElasticDriverMessageImpl(dest, data);

                        LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2}", dest, _rootTaskId, _iteration);
                        messages.Add(returnMessage);

                        _totNumberofNodes += _tasksInRing.Count;
                    }
                    else if (_currentWaitingList.Count > 0)
                    {
                        var dest = _ringHead.TaskId;
                        var nextTask = _currentWaitingList.First();
                        var data = new RingMessagePayload(nextTask, StageName, OperatorId, _iteration);
                        var returnMessage = new ElasticDriverMessageImpl(dest, data);

                        messages.Add(returnMessage);
                        LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2}", dest, nextTask, _iteration);

                        _ringHead.Next = new RingNode(nextTask, _iteration, _ringHead);
                        _ringHead = _ringHead.Next;
                        _tasksInRing.Add(nextTask);
                        _currentWaitingList.Remove(nextTask);
                    }
                }
                else
                {
                    var node = _ringHead;

                    while (node.Prev != null)
                    {
                        if (node.Prev.TaskId == taskId)
                        {
                            var dest = node.Prev;
                            var nextTask = node.TaskId;
                            var data = new RingMessagePayload(nextTask, StageName, OperatorId, _iteration);
                            var returnMessage = new ElasticDriverMessageImpl(dest.TaskId, data);

                            messages.Add(returnMessage);
                            LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2}", dest.TaskId, nextTask, _iteration);
                            return;
                        }

                        node = node.Prev;
                    }

                    LOGGER.Log(Level.Warning, "Task {0} was not found", taskId);
                }
            }
        }

        public void OnNewIteration(int iteration)
        {
            LOGGER.Log(Level.Info, string.Format("Ring in Iteration {0} is closed with {1} nodes", _iteration, _tasksInRing.Count));

            _ringHead.Next = new RingNode(_rootTaskId, _iteration, _ringHead);
            _ringHead = _ringHead.Next;
            _currentWaitingList = _nextWaitingList;
            _nextWaitingList = new HashSet<string>();
            _tasksInRing = new HashSet<string> { { _rootTaskId } };
            _prevRingHeads.Add(_iteration, _ringHead);

            _iteration = iteration;
            CleanPreviousRings();
        }

        public string LogFinalStatistics()
        {
            return string.Format("\nAverage number of nodes in ring {0}", (float)_totNumberofNodes / (_iteration > 2 ? _iteration - 1 : 1));
        }

        internal void ResumeRing(ref List<IElasticDriverMessage> returnMessages)
        {
            TopologyUpdateResponse(_ringHead.TaskId, ref returnMessages, null);
        }

        internal void ResumeRing(ref List<IElasticDriverMessage> returnMessages, string taskId, int iteration)
        {
            var node = _ringHead;

            while (node != null)
            {
                if (node.TaskId == taskId && node.Iteration == iteration)
                {
                    var dest = node.Prev.TaskId;
                    var data = new ResumeMessagePayload(node.TaskId, StageName, OperatorId, node.Iteration);
                    returnMessages.Add(new ElasticDriverMessageImpl(dest, data));
                    LOGGER.Log(Level.Info, "Task {0} sends to {1} in iteration {2}", dest, node.TaskId, node.Iteration);
                    return;
                }

                node = node.Prev;
            }

            LOGGER.Log(Level.Warning, "{0} in iteration {1} not found: ignoring", taskId, iteration); 
        }

        private void CleanPreviousRings()
        {
            lock (_lock)
            {
                if (_prevRingHeads.Count > 2)
                {
                    var smallerDict = _prevRingHeads.First();
                    var task = smallerDict.Value;
                    if (task.Next != null)
                    {
                        task.Next.Prev = null;
                    }

                    _prevRingHeads.Remove(smallerDict.Key);
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
            Type = DriverMessagePayloadType.Ring;
        }

        public string TaskId { get; private set; }

        public int Iteration { get; set; }

        public RingNode Next { get; set; }

        public RingNode Prev { get; set; }

        public DriverMessagePayloadType Type { get; set; }
    }
}
