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
        private string _taskSubscription;

        private volatile int _availableDataPoints;
        private int _totNumberofNodes;

        private readonly IDictionary<int, DataNode> _nodes;
        private readonly HashSet<string> _lostNodesToBeRemoved;
        private readonly HashSet<string> _nodesWaitingToJoinTopology;
        private readonly HashSet<string> _nodesWaitingToJoinTopologyInNextIteration;

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
            _taskSubscription = string.Empty;
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

        public string SubscriptionName { get; set; }

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

            confBuilder.BindNamedParameter<GroupCommunicationConfigurationOptions.TopologyRootTaskId, int>(
                    GenericType<GroupCommunicationConfigurationOptions.TopologyRootTaskId>.Class,
                    _rootId.ToString(CultureInfo.InvariantCulture));

            foreach (DataNode childNode in node.Children)
            {
                confBuilder.BindSetEntry<GroupCommunicationConfigurationOptions.TopologyChildTaskIds, int>(
                    GenericType<GroupCommunicationConfigurationOptions.TopologyChildTaskIds>.Class,
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
                    // New node but elastically added. It should be gracefully added to the ring.
                    _nodesWaitingToJoinTopologyInNextIteration.Add(taskId);
                    _nodes[id].FailState = DataNodeState.Unreachable;
                    failureMachine.AddDataPoints(1, true);
                    failureMachine.RemoveDataPoints(1);
                    return false;
                }

                // This is required later in order to build the topology
                if (_taskSubscription == string.Empty)
                {
                    _taskSubscription = Utils.GetTaskSubscriptions(taskId);
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
                node.Parent = null;
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

            if (SubscriptionName == string.Empty)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any subscription");
            }

            IEnumerator<DataNode> iter = _sorted ? _nodes.OrderBy(kv => kv.Key).Select(kv => kv.Value).GetEnumerator() : _nodes.Values.GetEnumerator();
            Queue<DataNode> parents = new Queue<DataNode>();
            var root = _nodes[_rootId];
            parents.Enqueue(root);

            BuildTopology(ref parents, ref iter);

            _rootTaskId = Utils.BuildTaskId(_taskSubscription, _rootId);
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
            if (taskId == _rootTaskId)
            {
                if (!failureStateMachine.IsPresent())
                {
                    throw new IllegalStateException("Cannot update topology without failure machine");
                }
                lock (_lock)
                {
                    var updates = new List<List<string>>();
                    var queue = new Queue<int>();
                    queue.Enqueue(_rootId);
                    BuildTopologyUpdateMessage(queue, updates, failureStateMachine.Value);

                    var data = new TopologyMessagePayload(updates, false, SubscriptionName, OperatorId, _iteration);
                    var returnMessage = new ElasticDriverMessageImpl(taskId, data);
                    returnMessages.Add(returnMessage);
                }
            }
            else
            {
                throw new IllegalStateException(string.Format("Not {0} but only root tasks are supposed to request topology updates", taskId));
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
            }
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
                int iter;

                if (info.IsPresent())
                {
                    iter = int.Parse(info.Value.Split(':')[0]);
                }
                else
                {
                    iter = iteration.Value;
                }

                var data = new TopologyMessagePayload(new List<List<string>>() { _lostNodesToBeRemoved.ToList() }, true, SubscriptionName, OperatorId, -1);
                var returnMessage = new ElasticDriverMessageImpl(_rootTaskId, data);

                LOGGER.Log(Level.Info, "Task {0} is removed from topology", taskId);
                messages.Add(returnMessage);
                _lostNodesToBeRemoved.Clear();
            }

            return messages;
        }

        public string LogFinalStatistics()
        {
            return string.Format("\nAverage number of nodes in the topology of Operator {0}: {1}", OperatorId, (float)_totNumberofNodes / (_iteration > 2 ? _iteration - 1 : 1));
        }

        private void BuildTopology(ref Queue<DataNode> parents, ref IEnumerator<DataNode> iter)
        {
            int i = 0;
            DataNode parent = parents.Dequeue();
            while (i < _degree && iter.MoveNext())
            {
                if (iter.Current.TaskId != _rootId)
                {
                    parent.AddChild(iter.Current);
                    iter.Current.Parent = parent;
                    parents.Enqueue(iter.Current);
                    i++;
                }
            }

            if (i == _degree)
            {
                BuildTopology(ref parents, ref iter);
            }
        }

        private void RemoveReachable(IEnumerator<DataNode> children, ref int count)
        {
            lock (_lock)
            {
                while (children.MoveNext())
                {
                    if (children.Current.FailState == DataNodeState.Reachable)
                    {
                        var taskId = Utils.BuildTaskId(_taskSubscription, children.Current.TaskId);
                        children.Current.FailState = DataNodeState.Unreachable;
                        children.Current.Parent = null;
                        _nodesWaitingToJoinTopologyInNextIteration.Add(taskId);
                        count++;

                        var nextChildren = children.Current.Children.GetEnumerator();
                        RemoveReachable(nextChildren, ref count);
                        children.Current.Children.Clear();
                    }
                }
            }
        }

        private void BuildTopologyUpdateMessage(Queue<int> ids, List<List<string>> updates, IFailureStateMachine failureStateMachine)
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
                        var taskId = Utils.BuildTaskId(_taskSubscription, id);

                        LOGGER.Log(Level.Info, string.Format("Tasks [{0}] are added to node {1} in iteration {2}", string.Join(",", children), taskId, _iteration));

                        var buffer = new List<string> { Utils.BuildTaskId(_taskSubscription, id) };

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
                        }
                        updates.Add(buffer);
                    }
                    foreach (var child in _nodes[id].Children)
                    {
                        if (child.FailState == DataNodeState.Reachable)
                        {
                            ids.Enqueue(child.TaskId);
                        }
                    }

                    BuildTopologyUpdateMessage(ids, updates, failureStateMachine);
                }
            }
        }
    }
}
