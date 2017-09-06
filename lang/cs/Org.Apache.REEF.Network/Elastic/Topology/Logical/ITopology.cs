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

using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical
{
    /// <summary>
    /// Represents a topology graph for Elastic Group Communication Operators.
    /// </summary>
    public interface ITopology
    {
        /// <summary>
        /// Adds a new task to the topology.
        /// When called before Build() actually adds the task to the topology.
        /// After Build(), it assumes that the task is added because recovered from a failure.
        /// </summary>
        /// <param name="taskId">The id of the task to be added</param>
        /// <returns>The number of data points linked with the added task id</returns>
        int AddTask(string taskId);

        /// <summary>
        /// Removes a task from the topology
        /// </summary>
        /// <param name="taskId">The id of the task to be removed</param>
        /// <returns>The number of data points lost because of the removed task id</returns>
        int RemoveTask(string taskId);

        /// <summary>
        /// Finalizes the Topology.
        /// After the Topology has been finalized, any task added to the topology is
        /// assumed as a task recovered from a failure
        /// </summary>
        /// <returns>The same finalized Subscription</returns>
        ITopology Build();

        void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId);

        string LogTopologyState();
    }
}
