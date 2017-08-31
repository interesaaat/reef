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

using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    /// <summary>
    /// Represents a node in the operator topology graph.
    /// </summary>
    internal sealed class DataNode
    {
        private int _taskId;

        private DataNode _parent;
        private bool _isRoot;
        private DataNodeState _state;

        private readonly List<DataNode> _children;

        public DataNode(
            int taskId, 
            bool isRoot)
        {
            _taskId = taskId;
            _isRoot = isRoot;
            _state = DataNodeState.Reachable;

            _children = new List<DataNode>();
        }

        public DataNodeState FailState
        {
            get { return _state; }
            set { _state = value; }
        }

        public DataNode Parent
        {
            get { return _parent; }
            set { _parent = value; }
        }

        public void AddChild(DataNode child)
        {
            _children.Add(child);
        }

        public int TaskId
        {
            get { return _taskId; }
        }

        public int NumberOfChildren
        {
            get { return _children.Count; }
        }

        public IList<DataNode> Children
        {
            get { return _children; }
        } 
    }
}
