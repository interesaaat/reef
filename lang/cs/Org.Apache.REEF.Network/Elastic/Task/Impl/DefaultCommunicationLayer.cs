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
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Utilities.Logging;
using System.Threading;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Implementation of the communication layer with default task to driver messages.
    /// </summary>
    internal sealed class DefaultCommunicationLayer : 
        CommunicationLayer,
        IDefaultTaskToDriverMessages
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultCommunicationLayer));

        /// <summary>
        /// Creates a new communication layer.
        /// </summary>
        [Inject]
        private DefaultCommunicationLayer(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.RetryCountWaitingForRegistration))] int retryRegistration,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SleepTimeWaitingForRegistration))] int sleepTime,
            [Parameter(typeof(ElasticServiceConfigurationOptions.SendRetry))] int retrySending,
            StreamingNetworkService<ElasticGroupCommunicationMessage> networkService,
            DefaultTaskToDriverMessageDispatcher taskToDriverDispatcher,
            ElasticDriverMessageHandler driverMessagesHandler,
            CentralizedCheckpointLayer checkpointService,
            IIdentifierFactory idFactory) : base(
                timeout,
                retryRegistration,
                sleepTime,
                retrySending,
                networkService,
                taskToDriverDispatcher,
                driverMessagesHandler,
                checkpointService,
                idFactory)
        {
        }

        /// <summary>
        /// Forward the received message to the target <see cref="IOperatorTopologyWithCommunication"/>.
        /// </summary>
        /// <param name="remoteMessage">The received message</param>
        public override void OnNext(IRemoteMessage<NsMessage<ElasticGroupCommunicationMessage>> remoteMessage)
        {
            if (_disposed)
            {
                LOGGER.Log(Level.Warning, "Received message after disposing: Ignoring.");
                return;
            }

            var nsMessage = remoteMessage.Message;
            var gcm = nsMessage.Data;
            var gcMessageTaskSource = nsMessage.SourceId.ToString();

            if (gcm.GetType() == typeof(CheckpointMessageRequest))
            {
                LOGGER.Log(Level.Info, "Received checkpoint request from " + gcMessageTaskSource);

                var cpm = gcm as CheckpointMessageRequest;
                ICheckpointState checkpoint;
                if (_checkpointService.GetCheckpoint(out checkpoint, nsMessage.DestId.ToString(), cpm.StageName, cpm.OperatorId, cpm.Iteration))
                {
                    CheckpointMessage returnMessage = checkpoint.ToMessage() as CheckpointMessage;
                    var cancellationSource = new CancellationTokenSource();

                    returnMessage.Checkpoint = checkpoint;

                    Send(gcMessageTaskSource, returnMessage, cancellationSource);
                }

                return;
            }
            if (gcm.GetType() == typeof(CheckpointMessage))
            {
                LOGGER.Log(Level.Info, "Received checkpoint from " + gcMessageTaskSource);
                var cpm = gcm as CheckpointMessage;
                _checkpointService.Checkpoint(cpm.Checkpoint);
                return;
            }
            
            // Data message
            var id = NodeObserverIdentifier.FromMessage(gcm);
            IOperatorTopologyWithCommunication operatorObserver;

            if (!_groupMessageObservers.TryGetValue(id, out operatorObserver))
            {
                throw new KeyNotFoundException($"Unable to find registered operator topology for stage {gcm.StageName} operator {gcm.OperatorId}");
            }

            operatorObserver.OnNext(nsMessage);
        }

        /// <summary>
        /// Notify the driver that a new iteration has begun.
        /// </summary>
        /// <param name="taskId">The current ask identifier</param>
        /// <param name="operatorId">The oeprator notfying the new iteration</param>
        /// <param name="iteration">The new iteration number</param>
        public void IterationNumber(string taskId, string stageName, int operatorId, int iteration)
        {
            _taskToDriverDispatcher.IterationNumber(taskId, stageName, operatorId, iteration);
        }

        /// <summary>
        /// Notify the driver that operator <see cref="operatorId"/> is ready to join the
        /// group communication topology.
        /// </summary>
        /// <param name="taskId">The current task</param>
        /// <param name="operatorId">The identifier of the operator ready to join the topology</param>
        public void JoinTopology(string taskId, string stageName, int operatorId)
        {
            _taskToDriverDispatcher.JoinTopology(taskId, stageName, operatorId);
        }

        /// <summary>
        /// Send a notification to the driver for an update on topology state.
        /// </summary>
        /// <param name="taskId">The current task id</param>
        /// <param name="operatorId">The operator requiring the topology update</param>
        public void TopologyUpdateRequest(string taskId, string stageName, int operatorId)
        {
            _taskToDriverDispatcher.TopologyUpdateRequest(taskId, stageName, operatorId);
        }

        /// <summary>
        /// Notify the driver that the current task is ready to accept new incoming data.
        /// </summary>
        /// <param name="taskId">The current task id</param>
        /// <param name="iteration">The current iteration number</param>
        public void NextDataRequest(string taskId, string stageName, int iteration = -1)
        {
            _taskToDriverDispatcher.NextDataRequest(taskId, stageName, iteration);
        }

        /// <summary>
        /// Signal the driver that the current stage is completed.
        /// </summary>
        /// <param name="taskId">The current task identifier</param>
        public void StageComplete(string taskId, string stageName)
        {
            _taskToDriverDispatcher.StageComplete(taskId, stageName);
        }
    }
}