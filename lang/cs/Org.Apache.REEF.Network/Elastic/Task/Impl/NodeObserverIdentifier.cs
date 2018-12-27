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

using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// An identifier for a given node in the group communication graph.
    /// A node is uniquely identifiable by a combination of its Task ID, 
    /// <see cref="StageName"/>, and <see cref="OperatorName"/>.
    /// </summary>
    internal sealed class NodeObserverIdentifier
    {
        private readonly string _stageName;
        private readonly int _operatorId;

        /// <summary>
        /// Creates a NodeObserverIdentifier from an observer.
        /// </summary>
        public static NodeObserverIdentifier FromObserver(OperatorTopologyWithCommunication observer)
        {
            return new NodeObserverIdentifier(observer.StageName, observer.OperatorId);
        }

        /// <summary>
        /// Creates a NodeObserverIdentifier from an observer.
        /// </summary>
        public static NodeObserverIdentifier FromObserver(DriverAwareOperatorTopology observer)
        {
            return new NodeObserverIdentifier(observer.StageName, observer.OperatorId);
        }

        /// <summary>
        /// Creates a NodeObserverIdentifier from a group communication message.
        /// </summary>
        public static NodeObserverIdentifier FromMessage(GroupCommunicationMessage message)
        {
            return new NodeObserverIdentifier(message.StageName, message.OperatorId);
        }

        private NodeObserverIdentifier(string stageName, int operatorName)
        {
            _stageName = stageName;
            _operatorId = operatorName;
        }

        /// <summary>
        /// The group name of the node.
        /// </summary>
        public string StageName
        {
            get { return _stageName; }
        }

        /// <summary>
        /// The operator name of the node.
        /// </summary>
        public int OperatorId
        {
            get { return _operatorId; }
        }

        /// <summary>
        /// Overrides <see cref="Equals"/>. Simply compares equivalence of instance fields.
        /// </summary>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj is NodeObserverIdentifier && Equals((NodeObserverIdentifier)obj);
        }

        /// <summary>
        /// Overrides <see cref="GetHashCode"/>. Generates hashcode based on the instance fields.
        /// </summary>
        public override int GetHashCode()
        {
            int hash = 17;
            hash = (hash * 31) + _stageName.GetHashCode();
            return (hash * 31) + _operatorId.GetHashCode();
        }

        /// <summary>
        /// Compare equality of instance fields.
        /// </summary>
        private bool Equals(NodeObserverIdentifier other)
        {
            return _stageName.Equals(other.StageName) &&
                _operatorId.Equals(other.OperatorId);
        }
    }
}