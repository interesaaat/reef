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

            DataNode parent = node.Parent;
            int parentId;

            if (parent == null)
            {
                parentId = node.TaskId;
            }
            else
            {
                parentId = parent.TaskId;
            }

            confBuilder.BindNamedParameter<GroupCommunicationConfigurationOptions.TopologyRootTaskId, int>(
                    GenericType<GroupCommunicationConfigurationOptions.TopologyRootTaskId>.Class,
                    parentId.ToString(CultureInfo.InvariantCulture));

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

                        failureMachine.AddDataPoints(0, false);
                        return false;
                    }

                    throw new ArgumentException("Task has already been added to the topology");
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

                if (node.FailState != DataNodeState.Unreachable)
                {
                    return 0;
                }

                node.FailState = node.FailState = DataNodeState.Lost;
                _nodesWaitingToJoinTopology.Remove(taskId);
                _nodesWaitingToJoinTopologyInNextIteration.Remove(taskId);
                _lostNodesToBeRemoved.Add(taskId);

                var children = node.Children.GetEnumerator();
                RemoveReachable(children, ref count);
                _availableDataPoints -= count;
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
            Queue<DataNode> current = new Queue<DataNode>();
            Queue<DataNode> next;
            var root = _nodes[_rootId];
            current.Enqueue(root);
            string output = string.Empty;

            while (current.Count != 0)
            {
                var iter = current.GetEnumerator();
                next = new Queue<DataNode>();
                while (iter.MoveNext())
                {
                    var rep = "X";
                    if (iter.Current.FailState == DataNodeState.Reachable)
                    {
                        rep = iter.Current.TaskId.ToString();
                    }

                    output += rep + " ";

                    foreach (var item in iter.Current.Children)
                    {
                        next.Enqueue(item);
                    }
                }
                if (next.Count > 0)
                {
                    output += "\n";
                }
                current = next;
            }

            return output;
        }

        public void TopologyUpdateResponse(string taskId, ref List<IElasticDriverMessage> returnMessages, Optional<IFailureStateMachine> failureStateMachine)
        {
            if (taskId == _rootTaskId)
            { 
                lock (_lock)
                {
                    var updates = new List<List<string>>();
                    var queue = new Queue<int>();
                    queue.Enqueue(_rootId);
                    BuildTopologyUpdateMessage(ref queue, ref updates);

                    var data = new TopologyMessagePayload(updates, false, SubscriptionName, OperatorId, _iteration);
                    var returnMessage = new ElasticDriverMessageImpl(taskId, data);
                    returnMessages.Add(returnMessage);
                }
            }
            else
            {
                throw new IllegalStateException("Only root tasks are supposed to request topology updates");
            }
        }

        private void RemoveReachable(IEnumerator<DataNode> children, ref int count)
        {
            lock (_lock)
            {
                while (children.MoveNext())
                {
                    var taskId = Utils.BuildTaskId(_taskSubscription, children.Current.TaskId);
                    children.Current.FailState = DataNodeState.Unreachable;
                    _nodesWaitingToJoinTopologyInNextIteration.Add(taskId);
                    count++;

                    var nextChildren = children.Current.Children.GetEnumerator();
                    RemoveReachable(nextChildren, ref count);
                    children.Current.Children.Clear();
                }
            }
        }

        public void OnNewIteration(int iteration)
        {
            LOGGER.Log(Level.Info, string.Format("Tree Topology for Operator {0} in Iteration {1} is closed with {2} nodes", OperatorId, iteration - 1, _availableDataPoints));

            _iteration = iteration;

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
            }

            return messages;
        }

        public string LogFinalStatistics()
        {
            return string.Empty;
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

        private void BuildTopologyUpdateMessage(ref Queue<int> ids, ref List<List<string>> updates)
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
                    var numNodesToAdd = _degree - _nodes[id].NumberOfChildren;

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
                            buffer.Add(child);
                            _nodesWaitingToJoinTopology.Remove(child);
                            _availableDataPoints++;
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

                    BuildTopologyUpdateMessage(ref ids, ref updates);
                }
            }
        }
    }
}
