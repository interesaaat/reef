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
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Globalization;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using Org.Apache.REEF.Tang.Exceptions;

namespace Org.Apache.REEF.Network.Elastic.Topology.Impl
{
    class TreeTopology : ITopology
    {
        private readonly int _rootId;
        private DataNode _root;
        private readonly int _degree;
        private bool _finalized;

        private readonly IDictionary<int, DataNode> _nodes;

        public TreeTopology(
            int rootId,
            int degree = 2,
            bool sorted = false)
        {
            _rootId = rootId;

            _degree = degree;

            if (sorted)
            {
                _nodes = new SortedDictionary<int, DataNode>();
            }
            else
            {
                _nodes = new Dictionary<int, DataNode>();
            }
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            DataNode selfTaskNode = GetTaskNode(taskId);
            if (selfTaskNode == null)
            {
                throw new ArgumentException("Task has not been added to the topology");
            }

            DataNode parent = selfTaskNode.Parent;
            int parentId;

            if (parent == null)
            {
                parentId = selfTaskNode.TaskId;
            }
            else
            {
                parentId = parent.TaskId;
            }

            confBuilder.BindNamedParameter<GroupCommConfigurationOptions.TopologyRootTaskId, int>(
                    GenericType<GroupCommConfigurationOptions.TopologyRootTaskId>.Class,
                    parentId.ToString(CultureInfo.InvariantCulture));

            foreach (DataNode childNode in selfTaskNode.Children)
            {
                confBuilder.BindSetEntry<GroupCommConfigurationOptions.TopologyChildTaskIds, int>(
                    GenericType<GroupCommConfigurationOptions.TopologyChildTaskIds>.Class,
                    childNode.TaskId.ToString(CultureInfo.InvariantCulture));
            }
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
                if (_finalized && _nodes[id].FailState != DataNodeState.Reachable)
                {
                    _nodes[id].FailState = DataNodeState.Reachable;

                    var children = _nodes[id].Children.GetEnumerator();

                    AddReachable(children);

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

            if (!_finalized)
            {
                throw new IllegalStateException("Removing task from a not finalized topology");
            }

            var id = Utils.GetTaskNum(taskId) - 1;

            if (!_nodes.ContainsKey(id))
            {
                throw new ArgumentException("Task is not part of this topology");
            }

            DataNode node = _nodes[id];

            if (node.FailState != DataNodeState.Reachable)
            {
                return 0;
            }

            node.FailState = node.FailState = DataNodeState.Lost;

            int count = 1;
            var children = node.Children.GetEnumerator();

            RemoveReachable(children, ref count);

            return count;
        }

        public void Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Topology cannot be built more than once");
            }

            IEnumerator<DataNode> iter = _nodes.Values.GetEnumerator();
            BuildTree(_root, ref iter);

            _finalized = true;
        }

        private void BuildTree(DataNode parent, ref IEnumerator<DataNode> iter)
        {
            int i = 0;

            while (i < _degree && iter.MoveNext())
            {
                parent.AddChild(iter.Current);
                iter.Current.Parent = parent;
                i++;
            }

            if (i == _degree)
            {
                var parentIter = parent.Children.GetEnumerator();

                while (parentIter.MoveNext())
                {
                    BuildTree(parentIter.Current, ref iter);
                }
            }
        }

        private DataNode GetTaskNode(int taskId)
        {
            if (_nodes.TryGetValue(taskId, out DataNode n))
            {
                return n;
            }
            throw new ArgumentException("Cannot find task node in the nodes.");
        }

        private void AddChild(int childId)
        {
            DataNode childNode = new DataNode(childId, false);
            _nodes[childId] = childNode;
        }

        private void SetRootNode(int rootId)
        {
            DataNode rootNode = new DataNode(rootId, true);
            _root = rootNode;
        }

        private void AddReachable(IEnumerator<DataNode> children)
        {
            while (children.MoveNext())
            {
                children.Current.FailState = DataNodeState.Reachable;

                var nextChildren = children.Current.Children.GetEnumerator();
                AddReachable(nextChildren);
            }
        }

        private void RemoveReachable(IEnumerator<DataNode> children, ref int count)
        {
            while (children.MoveNext())
            {
                children.Current.FailState = DataNodeState.Unreachable;
                count++;

                var nextChildren = children.Current.Children.GetEnumerator();
                AddReachable(nextChildren);
            }
        }
    }
}
