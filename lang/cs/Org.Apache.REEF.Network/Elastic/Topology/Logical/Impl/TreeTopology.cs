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
using Org.Apache.REEF.Network.Elastic.Config;
using System.Globalization;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tang.Exceptions;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Enum;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    public class TreeTopology : ITopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TreeTopology));

        private int _rootId;
        private string _rootTaskId;
        private readonly int _degree;
        private readonly bool _sorted;
        private bool _finalized;
        private volatile int _iteration;
        private string _taskStage;

        private volatile int _availableDataPoints;
        private int _totNumberofNodes;

        private readonly IDictionary<int, DataNode> _nodes;
        private readonly HashSet<string> _lostNodesToBeRemoved;
        private readonly HashSet<string> _nodesWaitingToJoinTopology;
        private readonly HashSet<string> _nodesWaitingToJoinTopologyInNextIteration;

        private List<TopologyUpdate> _cachedMessageUpdates;

        private readonly object _lock;

        public TreeTopology(
            int rootId,
            int degree = 2,
            bool sorted = false)
        {
            _rootId = rootId;
            _sorted = sorted;
            _degree = degree;
            OperatorId = -1;
            _taskStage = string.Empty;
            _rootTaskId = string.Empty;

            _availableDataPoints = 0;
            _totNumberofNodes = 0;

            _nodes = new Dictionary<int, DataNode>();
            _lostNodesToBeRemoved = new HashSet<string>();
            _nodesWaitingToJoinTopology = new HashSet<string>();
            _nodesWaitingToJoinTopologyInNextIteration = new HashSet<string>();

            _lock = new object();
        }

        public int OperatorId { get; set; }

        public string StageName { get; set; }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            if (!_nodes.TryGetValue(taskId, out DataNode node))
            {
                throw new ArgumentException("Cannot find task node " + taskId + " in the nodes.");
            }
           
            if (node == null)
            {
                throw new ArgumentException("Task has not been added to the topology");
            }

            var rootId = node.Parent != null ? node.Parent.TaskId : _rootId;

            confBuilder.BindNamedParameter<OperatorParameters.TopologyRootTaskId, int>(
                    GenericType<OperatorParameters.TopologyRootTaskId>.Class,
                    rootId.ToString(CultureInfo.InvariantCulture));

            foreach (DataNode childNode in node.Children)
            {
                confBuilder.BindSetEntry<OperatorParameters.TopologyChildTaskIds, int>(
                    GenericType<OperatorParameters.TopologyChildTaskIds>.Class,
                    childNode.TaskId.ToString(CultureInfo.InvariantCulture));
            }
        }

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
                    if (_nodes[id].FailState != DataNodeState.Reachable)
                    {
                        _nodesWaitingToJoinTopologyInNextIteration.Add(taskId);
                        _nodes[id].FailState = DataNodeState.Unreachable;
                        return false;
                    }

                    throw new ArgumentException("{0} has already been added to the topology", taskId);
                }

                DataNode node = new DataNode(id, true);
                _nodes[id] = node;

                if (_finalized)
                {
                    // New node but elastically added. It should be gracefully added to the tree.
                    _nodesWaitingToJoinTopologyInNextIteration.Add(taskId);
                    _nodes[id].FailState = DataNodeState.Unreachable;
                    failureMachine.AddDataPoints(1, true);
                    failureMachine.RemoveDataPoints(1);
                    return false;
                }

                // This is required later in order to build the topology
                if (_taskStage == string.Empty)
                {
                    _taskStage = Utils.GetTaskStages(taskId);
                }
            }

            _availableDataPoints++;
            failureMachine.AddDataPoints(1, true);

            return true;
        }

        public int RemoveTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            if (!_finalized)
            {
                throw new IllegalStateException("Removing task from a not finalized topology");
            }

            if (taskId == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet");
            }

            var id = Utils.GetTaskNum(taskId);
            int count = 1;

            lock (_lock)
            {
                if (!_nodes.ContainsKey(id))
                {
                    throw new ArgumentException("Task is not part of this topology");
                }

                DataNode node = _nodes[id];
                var state = node.FailState;
                node.FailState = DataNodeState.Lost;
                _nodesWaitingToJoinTopology.Remove(taskId);
                _nodesWaitingToJoinTopologyInNextIteration.Remove(taskId);

                if (state != DataNodeState.Lost)
                {
                    _lostNodesToBeRemoved.Add(taskId);
                }

                if (state != DataNodeState.Reachable)
                {
                    return 0;
                }

                var parent = node.Parent;
                parent.Children.Remove(node);
                var children = node.Children.GetEnumerator();
                RemoveReachable(children, ref count);
                _availableDataPoints -= count;
                node.Children.Clear();
            }

            return count;
        }

        public bool CanBeScheduled()
        {
            return _nodes.ContainsKey(_rootId);
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

            if (StageName == string.Empty)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any stage");
            }

            IEnumerator<DataNode> iter = _sorted ? _nodes.Where(x => x.Key != _rootId).OrderBy(kv => kv.Key).Select(kv => kv.Value).GetEnumerator() : _nodes.Where(x => x.Key != _rootId).Select(x => x.Value).GetEnumerator();
            var root = _nodes[_rootId];
            List<DataNode> parents = new List<DataNode>() { root };

            BuildTopology(parents, ref iter);

            _rootTaskId = Utils.BuildTaskId(_taskStage, _rootId);
            _finalized = true;

            return this;
        }

        public string LogTopologyState()
        {
            string output = string.Empty;

            lock (_lock)
            {
                foreach (var node in _nodes.OrderBy(x => x.Key).Select(x => x.Value))
                {
                    if (output != string.Empty)
                    {
                        output += "\n";
                    }
                    var rep = "X";
                    if (node.FailState == DataNodeState.Reachable)
                    {
                        rep = node.TaskId.ToString();
                    }

                    output += rep;

                    if (node.Children.Count > 0)
                    {
                        output += " -> " + string.Join(",", node.Children.Select(x => x.TaskId));
                    }
                }
            }

            return output;
        }

        public void TopologyUpdateResponse(string taskId, ref List<IElasticDriverMessage> returnMessages, Optional<IFailureStateMachine> failureStateMachine)
        {
            if (!failureStateMachine.IsPresent())
            {
                throw new IllegalStateException("Cannot update topology without failure machine");
            }
            lock (_lock)
            {
                if (_cachedMessageUpdates == null)
                {
                    var tempUpdates = new Dictionary<int, TopologyUpdate>();
                    var queue = new Queue<int>();
                    queue.Enqueue(_rootId);
                    BuildTopologyUpdateResponse(queue, tempUpdates, failureStateMachine.Value);
                    _cachedMessageUpdates = tempUpdates.Values.ToList();
                }

                var data = new UpdateMessagePayload(_cachedMessageUpdates, StageName, OperatorId, _iteration);
                var returnMessage = new ElasticDriverMessageImpl(taskId, data);
                returnMessages.Add(returnMessage);
            }
        }

        public void OnNewIteration(int iteration)
        {
            LOGGER.Log(Level.Info, string.Format("Tree Topology for Operator {0} in Iteration {1} is closed with {2} nodes", OperatorId, iteration - 1, _availableDataPoints));
            _iteration = iteration;
            _totNumberofNodes += _availableDataPoints;

            lock (_lock)
            {
                foreach (var node in _nodesWaitingToJoinTopologyInNextIteration)
                {
                    _nodesWaitingToJoinTopology.Add(node);
                }

                _nodesWaitingToJoinTopologyInNextIteration.Clear();
                _cachedMessageUpdates = null; // Reset the updates
            }
        }

        public IList<IElasticDriverMessage> Reconfigure(string taskId, Optional<string> info, Optional<int> iteration)
        {
            List<IElasticDriverMessage> messages = new List<IElasticDriverMessage>();

            lock (_lock)
            {
                int iter;
                var updates = new List<TopologyUpdate>();

                if (info.IsPresent())
                {
                    iter = int.Parse(info.Value.Split(':')[0]);
                }
                else
                {
                    iter = iteration.Value;
                }

                foreach (var taskIdToRemove in _lostNodesToBeRemoved)
                {
                    var id = Utils.GetTaskNum(taskIdToRemove);
                    var node = _nodes[id];
                    if (node.Parent == null)
                    {
                        LOGGER.Log(Level.Warning, string.Format("Task {0} will not be reconfigured", taskIdToRemove));
                        continue;
                    }
                    
                    var parentTaskId = Utils.BuildTaskId(_taskStage, node.Parent.TaskId);

                    updates.Add(new TopologyUpdate(parentTaskId, new List<string>() { taskIdToRemove }));

                    var data = new FailureMessagePayload(updates, StageName, OperatorId, iter);
                    var returnMessage = new ElasticDriverMessageImpl(parentTaskId, data);

                    LOGGER.Log(Level.Info, "Task {0} is removed from topology of node {1}", taskIdToRemove, parentTaskId);
                    messages.Add(returnMessage);
                    node.Parent = null;
                }
                
                _lostNodesToBeRemoved.Clear();
            }

            return messages;
        }

        public string LogFinalStatistics()
        {
            return $"\nAverage number of nodes in the topology of Operator {OperatorId}: {(_iteration >= 2 ? (float)_totNumberofNodes / (_iteration - 1) : _availableDataPoints)}";
        }

        private void BuildTopology(List<DataNode> parents, ref IEnumerator<DataNode> iter)
        {
            var next = new List<DataNode>();
            
            for (int i = 0; i < _degree; i++)
            {
                for (int k = 0; k < parents.Count(); k++)
                { 
                    if (iter.MoveNext())
                    {
                        parents[k].AddChild(iter.Current);
                        iter.Current.Parent = parents[k];
                        next.Add(iter.Current);
                    }
                    else
                    {
                        return;
                    }
                }
            }

            BuildTopology(next, ref iter);
        }

        private void RemoveReachable(IEnumerator<DataNode> children, ref int count)
        {
            lock (_lock)
            {
                while (children.MoveNext())
                {
                    if (children.Current.FailState == DataNodeState.Reachable)
                    {
                        var taskId = Utils.BuildTaskId(_taskStage, children.Current.TaskId);
                        children.Current.FailState = DataNodeState.Unreachable;
                        _nodesWaitingToJoinTopologyInNextIteration.Add(taskId);
                        _lostNodesToBeRemoved.Add(taskId);
                        count++;

                        var nextChildren = children.Current.Children.GetEnumerator();
                        RemoveReachable(nextChildren, ref count);
                        children.Current.Children.Clear();
                    }
                }
            }
        }

        private void BuildTopologyUpdateResponse(Queue<int> ids, Dictionary<int, TopologyUpdate> updates, IFailureStateMachine failureStateMachine)
        {
            lock (_lock)
            {
                if (_nodesWaitingToJoinTopology.Count > 0)
                {
                    if (ids.Count == 0)
                    {
                        throw new IllegalStateException("Queue for adding nodes to the tree is empty");
                    }
                    var id = ids.Dequeue();
                    var numNodesToAdd = _degree - _nodes[id].Children.Where(x => x.FailState == DataNodeState.Reachable).Count();

                    if (numNodesToAdd > 0)
                    {
                        var children = _nodesWaitingToJoinTopology.Take(numNodesToAdd).ToArray();
                        var taskId = Utils.BuildTaskId(_taskStage, id);

                        LOGGER.Log(Level.Info, string.Format("Tasks [{0}] are added to node {1} in iteration {2}", string.Join(",", children), taskId, _iteration));

                        var buffer = new List<string>();

                        foreach (var child in children)
                        {
                            var childId = Utils.GetTaskNum(child);
                            _nodes[childId].FailState = DataNodeState.Reachable;
                            _nodes[id].Children.Add(_nodes[childId]);
                            _nodes[childId].Parent = _nodes[id];
                            buffer.Add(child);
                            _nodesWaitingToJoinTopology.Remove(child);
                            _availableDataPoints++;
                            failureStateMachine.AddDataPoints(1, false);
                            updates.Add(childId, new TopologyUpdate(child, taskId));
                        }

                        if (taskId == _rootTaskId)
                        {
                            updates.Add(id, new TopologyUpdate(Utils.BuildTaskId(_taskStage, id), buffer));
                        }
                        else
                        {
                            if (!updates.TryGetValue(id, out TopologyUpdate value))
                            {
                                value = new TopologyUpdate(Utils.BuildTaskId(_taskStage, id), buffer, Utils.BuildTaskId(_taskStage, _nodes[id].Parent.TaskId));
                                updates.Add(id, value);
                            }
                            else
                            {
                                value.Children = buffer;
                            }
                        }
                    }
                    foreach (var child in _nodes[id].Children)
                    {
                        if (child.FailState == DataNodeState.Reachable)
                        {
                            ids.Enqueue(child.TaskId);
                        }
                    }

                    BuildTopologyUpdateResponse(ids, updates, failureStateMachine);
                }
            }
        }
    }
}
