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

using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Driver.Policy;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Interface;
using System;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical
{
    /// <summary>
    /// Used to create Communication Groups for Group Communication Operators.
    /// Also manages configuration for Group Communication tasks/services.
    /// </summary>
    [System.Obsolete]
    public interface IElasticOperator
    {
        void AddTask(string taskId);

        void GetElasticTaskConfiguration(out ICsConfigurationBuilder confBuilder);

        void EnsurePolicy(PolicyLevel level);
    
        void Reset();

        /// <summary>
        /// Adds the Broadcast Group Communication operator to the communication group.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send</typeparam>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="configurations">The configuration for task</param>
        /// <param name="senderTaskId">The master task id in broadcast operator</param>
        /// <param name="topologyType">The topology type for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Broadcast operator info</returns>
        IElasticOperator Broadcast<T>(String operatorName, string senderTaskId, IElasticOperator prev, TopologyTypes topologyType, params IConfiguration[] configurations);

        /// <summary>
        /// Adds the Broadcast Group Communication operator to the communication group. Default to IntCodec
        /// </summary>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="senderTaskId">The master task id in broadcast operator</param>
        /// <param name="topologyType">The topology type for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Broadcast operator info</returns>
        IElasticOperator Broadcast(String operatorName, string senderTaskId, IElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat);

        /// <summary>
        /// Adds the Reduce Group Communication operator to the communication group.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send</typeparam>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="configurations">The configurations for task</param>
        /// <param name="receiverTaskId">The master task id for the typology</param>
        /// <param name="topologyType">The topology for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Reduce operator info</returns>
        IElasticOperator Reduce<T>(String operatorName, string receiverTaskId, IElasticOperator prev, TopologyTypes topologyType, params IConfiguration[] configurations);

        /// <summary>
        /// Adds the Scatter Group Communication operator to the communication group with default Codec
        /// </summary>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="senderTaskId">The sender id</param>
        /// <param name="topologyType">type of topology used in the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Scatter operator info</returns>
        IElasticOperator Scatter(String operatorName, string senderTaskId, IElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat);

        /// <summary>
        /// Adds the Scatter Group Communication operator to the communication group.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send</typeparam>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="condition">The sender id</param>
        /// <param name="configurations">The configuration for task</param>
        /// <returns>The same CommunicationGroupDriver with the added Scatter operator info</returns>
        IElasticOperator Iterate<T>(String operatorName, System.Delegate condition, IElasticOperator prev, params IConfiguration[] configurations);
    }
}
