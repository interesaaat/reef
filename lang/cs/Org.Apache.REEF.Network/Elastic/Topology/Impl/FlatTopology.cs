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
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Elastic.Topology.Impl
{
    class FlatTopology : ITopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(FlatTopology));

        private int _rootId;
        private DataNode _root;
        private bool _finalized;

        private readonly Dictionary<int, DataNode> _nodes;

        public FlatTopology(int rootId)
        {
            _rootId = rootId;
            _finalized = false;

            _nodes = new Dictionary<int, DataNode>();
        }

        public bool AddTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            var id = Utils.GetTaskNum(taskId) - 1;

            if (_nodes.ContainsKey(id))
            {
                if (_nodes[id].FailState != DataNodeState.Reachable)
                {
                    _nodes[id].FailState = DataNodeState.Reachable;
                    return true;
                }

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

            Log();

            _finalized = true;
        }

        private void Log()
        {
            LOGGER.Log(Level.Info, _rootId + "\n");

            var children = _root.Children.GetEnumerator();
            string output = "";
            while (children.MoveNext())
            {
                output += children.Current.TaskId + " ";
            }

            LOGGER.Log(Level.Info, output);
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            if (taskId == _rootId)
            {
                foreach (var tId in _root.Children)
                {
                    if (tId.TaskId != _rootId)
                    {
                        confBuilder.BindSetEntry<GroupCommConfigurationOptions.TopologyChildTaskIds, int>(
                            GenericType<GroupCommConfigurationOptions.TopologyChildTaskIds>.Class,
                            tId.TaskId.ToString(CultureInfo.InvariantCulture));
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
