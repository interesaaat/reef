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
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    public class FlatTopology : ITopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(FlatTopology));

        private string _rootTaskId;
        private int _rootId;
        private string _taskSubscription;
        private volatile int _iteration;
        private bool _finalized;
        private readonly bool _sorted;

        private readonly Dictionary<int, DataNode> _nodes;
        private readonly HashSet<string> _lostNodesToBeRemoved;
        private readonly HashSet<string> _nodesWaitingToJoinTopology;

        private readonly object _lock;

        public FlatTopology(int rootId, bool sorted = false)
        {
            _rootTaskId = string.Empty;
            _taskSubscription = string.Empty;
            _rootId = rootId;
            _finalized = false;
            _sorted = sorted;
            OperatorId = -1;
            _iteration = 1;

            _lock = new object();

            _nodes = new Dictionary<int, DataNode>();
            _lostNodesToBeRemoved = new HashSet<string>();
            _nodesWaitingToJoinTopology = new HashSet<string>();
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
                    if (_nodes[id].FailState != DataNodeState.Reachable)
                    {
                        _nodesWaitingToJoinTopology.Add(taskId);
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
                    _nodesWaitingToJoinTopology.Add(taskId);
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

            lock (_lock)
            {
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
                _nodesWaitingToJoinTopology.Remove(taskId);
                _lostNodesToBeRemoved.Add(taskId);
            }

            return 1;
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

            BuildTopology();

            _rootTaskId = Utils.BuildTaskId(_taskSubscription, _rootId);
            _finalized = true;

            return this;
        }

        public string LogTopologyState()
        {
            var root = _nodes[_rootId];
            var children = root.Children.GetEnumerator();
            string output = _rootId + "\n";
            while (children.MoveNext())
            {
                var rep = "X";
                if (children.Current.FailState == DataNodeState.Reachable)
                {
                    rep = children.Current.TaskId.ToString();
                }

                output += rep + " ";
            }

            return output;
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            if (taskId == _rootId)
            {
                var root = _nodes[_rootId];

                foreach (var tId in root.Children)
                {
                    if (tId.TaskId != _rootId)
                    {
                        confBuilder.BindSetEntry<GroupCommunicationConfigurationOptions.TopologyChildTaskIds, int>(
                            GenericType<GroupCommunicationConfigurationOptions.TopologyChildTaskIds>.Class,
                            tId.TaskId.ToString(CultureInfo.InvariantCulture));
                    }
                }
            }
            confBuilder.BindNamedParameter<GroupCommunicationConfigurationOptions.TopologyRootTaskId, int>(
                    GenericType<GroupCommunicationConfigurationOptions.TopologyRootTaskId>.Class,
                    _rootId.ToString(CultureInfo.InvariantCulture));
        }

        public void TopologyUpdateResponse(string taskId, ref List<IElasticDriverMessage> returnMessages)
        {
            if (taskId == _rootTaskId)
            {
                lock (_lock)
                {
                    var data = new TopologyMessagePayload(_nodesWaitingToJoinTopology.ToList(), false, SubscriptionName, OperatorId, _iteration);
                    var returnMessage = new ElasticDriverMessageImpl(_rootTaskId, data);

                    returnMessages.Add(returnMessage);

                    if (_nodesWaitingToJoinTopology.Count > 0)
                    {
                        LOGGER.Log(Level.Info, string.Format("Tasks {0} are added to topology in iteration {1}", string.Join(",", _nodesWaitingToJoinTopology), _iteration));
                        _nodesWaitingToJoinTopology.Clear();
                    }
                }
            }
        }

        public void OnNewIteration(int iteration)
        {
            _iteration = iteration;
        }

        public IList<IElasticDriverMessage> Reconfigure(string taskId, Optional<string> info, Optional<int> iteration)
        {
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

                var data = new TopologyMessagePayload(_lostNodesToBeRemoved.ToList(), true, SubscriptionName, OperatorId, -1);
                var returnMessage = new ElasticDriverMessageImpl(_rootTaskId, data);

                LOGGER.Log(Level.Info, "Task {0} is removed from topology", taskId);
                messages.Add(returnMessage);
            }

            return messages;
        }
    
        private void BuildTopology()
        {
            IEnumerator<DataNode> iter = _sorted ? _nodes.OrderBy(kv => kv.Key).Select(kv => kv.Value).GetEnumerator() : _nodes.Values.GetEnumerator();
            var root = _nodes[_rootId];

            while (iter.MoveNext())
            {
                if (iter.Current.TaskId != _rootId)
                {
                    root.AddChild(iter.Current);
                }
            }
        }
    }
}
