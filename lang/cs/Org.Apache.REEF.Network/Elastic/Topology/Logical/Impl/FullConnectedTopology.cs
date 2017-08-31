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

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    public class FullConnectedTopology : ITopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(FullConnectedTopology));

        private int _rootId;
        private bool _finalized;

        private readonly Dictionary<int, DataNode> _nodes;

        public FullConnectedTopology(int rootId)
        {
            _rootId = rootId;
            _finalized = false;

            _nodes = new Dictionary<int, DataNode>();
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

                    return 1;
                }

                throw new ArgumentException("Task has already been added to the topology");
            }

            DataNode node = new DataNode(id, false);
            _nodes[id] = node;

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

            return 1;
        }

        public void Build()
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

        private void BuildTopology()
        {
        }
    }
}
