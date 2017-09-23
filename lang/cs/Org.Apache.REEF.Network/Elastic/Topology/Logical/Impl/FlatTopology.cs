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
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using Org.Apache.REEF.Network.Elastic.Driver;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    public class FlatTopology : ITopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(FlatTopology));

        private int _rootId;
        private bool _finalized;
        private readonly bool _sorted;

        private readonly Dictionary<int, DataNode> _nodes;

        public FlatTopology(int rootId, bool sorted = false)
        {
            _rootId = rootId;
            _finalized = false;
            _sorted = sorted;
            OperatorId = -1;

            _nodes = new Dictionary<int, DataNode>();
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
            else
            {
                confBuilder.BindNamedParameter<GroupCommunicationConfigurationOptions.TopologyRootTaskId, int>(
                        GenericType<GroupCommunicationConfigurationOptions.TopologyRootTaskId>.Class,
                        _rootId.ToString(CultureInfo.InvariantCulture));
            }
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

        public List<IElasticDriverMessage> Reconfigure(string taskId, string info)
        {
            throw new NotImplementedException();
        }
    }
}
