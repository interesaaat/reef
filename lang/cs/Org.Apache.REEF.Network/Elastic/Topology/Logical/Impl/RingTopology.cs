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
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using System.Linq;

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

        private int _rootId;
        private string _rootTaskId;
        private bool _finalized;
        private string _subscription;

        private readonly Dictionary<int, DataNode> _nodes;

        private int _availableDataPoints;

        private HashSet<string> _currentWaitingList;
        private HashSet<string> _nextWaitingList;
        private HashSet<string> _tasksInRing;
        private LinkedList<string> _ring;
        private LinkedList<string> _prevRing;
        private string _ringPrint;
        private string _lastToken;

        private readonly object _lock;

        public RingTopology(int rootId)
        {
            _rootId = rootId;
            _rootTaskId = string.Empty;
            _subscription = string.Empty;
            _finalized = false;

            _nodes = new Dictionary<int, DataNode>();

            _availableDataPoints = 0;

            _currentWaitingList = new HashSet<string>();
            _nextWaitingList = new HashSet<string>();
            _ring = new LinkedList<string>();
            _prevRing = new LinkedList<string>();
            _ringPrint = string.Empty;
            _lastToken = string.Empty;

            _lock = new object();
        }

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
            if (_subscription == string.Empty)
            {
                _subscription = Utils.GetTaskSubscriptions(taskId);
            }

            return 1;
        }

        public ISet<DriverMessage> GetNextTasksInRing()
        {
            var messages = new HashSet<DriverMessage>();

            SubmitNextNodes(ref messages);

            return messages;
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
                throw new IllegalStateException("Topology cannot be built becasue the root node is missing");
            }

            BuildTopology();

            _finalized = true;

            return this;
        }

        public void AddTaskIdToRing(string taskId)
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

        public void UpdateTokenPosition(string taskId)
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
                    if (_ring.Contains(taskId))
                    {
                        if (_prevRing.Count > 0 && _lastToken == _prevRing.First.Value)
                        {
                            _prevRing.Clear();
                        }

                        var head = _ring.First;

                        while (head.Value != taskId)
                        {
                            head = head.Next;
                            _ring.RemoveFirst();
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

        public string LogTopologyState()
        {
            var nodes = _nodes.Values.GetEnumerator();
            string output = _rootId + "\n";
            while (nodes.MoveNext())
            {
                if (nodes.Current.TaskId != _rootId)
                {
                    var rep = "X";
                    if (nodes.Current.FailState == DataNodeState.Reachable)
                    {
                        rep = nodes.Current.TaskId.ToString();
                    }

                    output += rep + " ";
                }
            }

            return output;
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

        private void SubmitNextNodes(ref HashSet<DriverMessage> messages)
        {
            lock (_lock)
            {
                while (_currentWaitingList.Count > 0)
                {
                    var enumerator = _currentWaitingList.Take(1);
                    foreach (var nextTask in enumerator)
                    {
                        var dest = _ring.Last.Value;
                        var data = new RingMessagePayload(nextTask);
                        var returnMessage = new DriverMessage(dest, data);

                        messages.Add(returnMessage);
                        _ring.AddLast(nextTask);
                        _tasksInRing.Add(nextTask);
                        _currentWaitingList.Remove(nextTask);

                        _ringPrint += "->" + nextTask;
                    }
                }

                if (_availableDataPoints <= _tasksInRing.Count)
                {
                    var dest = _ring.Last.Value;
                    var data = new RingMessagePayload(_rootTaskId);
                    var returnMessage = new DriverMessage(dest, data);

                    messages.Add(returnMessage);
                    LOGGER.Log(Level.Info, "Ring is closed:\n {0}->{1}", _ringPrint, _rootTaskId);

                    _prevRing = _ring;
                    _ring = new LinkedList<string>();
                    _ring.AddLast(_rootTaskId);
                    _currentWaitingList = _nextWaitingList;
                    _nextWaitingList = new HashSet<string>();
                    _tasksInRing = new HashSet<string> { { _rootTaskId } };

                    foreach (var task in _currentWaitingList)
                    {
                        _tasksInRing.Add(task);
                    }

                    _ringPrint = _rootTaskId;
                }
            }

            // Continuously build the ring until there is some node waiting
            if (_currentWaitingList.Count > 0)
            {
                SubmitNextNodes(ref messages);
            }
        }

        private void BuildTopology()
        {
            _rootTaskId = Utils.BuildTaskId(_subscription, _rootId);
            _tasksInRing = new HashSet<string> { { _rootTaskId } };
            _ring.AddLast(_rootTaskId);
            _lastToken = _rootTaskId;

            _ringPrint = _rootTaskId;
        }
    }
}
