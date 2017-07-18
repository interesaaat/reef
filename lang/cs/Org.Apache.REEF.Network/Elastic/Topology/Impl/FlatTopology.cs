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
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Globalization;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;

namespace Org.Apache.REEF.Network.Elastic.Topology.Impl
{
    class FlatTopology : ITopology
    {
        private int _rootId;

        private readonly Dictionary<int, DataNode> _nodes;
        private DataNode _root;

        /// <summary>
        /// Creates a new FlatTopology.
        /// </summary>
        /// <param name="operatorName">The operator name</param>
        /// <param name="groupName">The name of the topology's CommunicationGroup</param>
        /// <param name="rootId">The root Task identifier</param>
        /// <param name="driverId">The driver identifier</param>
        /// <param name="operatorSpec">The operator specification</param>
        public FlatTopology(int rootId)
        {
            _rootId = rootId;

            _nodes = new Dictionary<int, DataNode>();
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            if (taskId == _rootId)
            {
                foreach (var tId in _nodes.Keys)
                {
                    if (!tId.Equals(_rootId))
                    {
                        confBuilder.BindSetEntry<GroupCommConfigurationOptions.TopologyChildTaskIds, int>(
                            GenericType<GroupCommConfigurationOptions.TopologyChildTaskIds>.Class,
                            tId.ToString(CultureInfo.InvariantCulture));
                    }
                }
            }
            else
            {
                confBuilder.BindNamedParameter<GroupCommConfigurationOptions.TopologyRootTaskId, int>(
                        GenericType<GroupCommConfigurationOptions.TopologyRootTaskId>.Class,
                        _rootId.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// Adds a task to the topology graph.
        /// </summary>
        /// <param name="taskId">The identifier of the task to add</param>
        public bool AddTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            var id = Utils.GetTaskNum(taskId) - 1;

            if (_nodes.ContainsKey(id))
            {
                throw new ArgumentException("Task has already been added to the topology");
            }

            if (id == _rootId)
            {
                SetRootNode(_rootId);
            }
            else
            {
                AddChild(id);
            }
            return true;
        }

        /// <summary>
        /// Adds a task to the topology graph.
        /// </summary>
        /// <param name="taskId">The identifier of the task to add</param>
        public int RemoveTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            var id = Utils.GetTaskNum(taskId) - 1;

            if (!_nodes.ContainsKey(id))
            {
                throw new ArgumentException("Task is not part of this topology");
            }

            DataNode node = _nodes[id];

            if (node.FailState)
            {
                return 0;
            }

            node.FailState = true;

            return 1;
        }

        private void SetRootNode(int rootId)
        {
            DataNode rootNode = new DataNode(rootId, true);
            _root = rootNode;

            foreach (DataNode childNode in _nodes.Values)
            {
                rootNode.AddChild(childNode);
                childNode.Parent = rootNode;
            }
        }

        private void AddChild(int childId)
        {
            DataNode childNode = new DataNode(childId, false);
            _nodes[childId] = childNode;

            if (_root != null)
            {
                _root.AddChild(childNode);
                childNode.Parent = _root;
            }
        }
    }
}
