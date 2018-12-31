﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using Org.Apache.REEF.Tang.Annotations;
using System;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Comm.Enum;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Implemention of <see cref="TaskToDriverMessageDispatcher"/> with default
    /// messages dispatcher.
    /// </summary>
    internal sealed class DefaultTaskToDriverMessageDispatcher : TaskToDriverMessageDispatcher, IDefaultTaskToDriverMessages
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskToDriverMessageDispatcher));

        /// <summary>
        /// Injectable constrcutor.
        /// </summary>
        /// <param name="heartBeatManager"></param>
        [Inject]
        private DefaultTaskToDriverMessageDispatcher(IInjector injector) : base(injector)
        {
        }

        /// <summary>
        /// Notify the driver that a new iteration has begun.
        /// </summary>
        /// <param name="taskId">The current ask identifier</param>
        /// <param name="operatorId">The oeprator notfying the new iteration</param>
        /// <param name="iteration">The new iteration number</param>
        public void IterationNumber(string taskId, string stageName, int operatorId, int iteration)
        {
            int offset = 0;
            byte[] message = new byte[sizeof(ushort) + stageName.Length + 6];
            Buffer.BlockCopy(BitConverter.GetBytes(stageName.Length), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(ByteUtilities.StringToByteArrays(stageName), 0, message, offset, stageName.Length);
            offset += stageName.Length;
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.IterationNumber), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)operatorId), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)iteration), 0, message, offset, sizeof(ushort));

            LOGGER.Log(Level.Info, $"Sending current iteration number ({iteration}) through heartbeat.");

            Send(taskId, message);
        }

        /// <summary>
        /// Notify the driver that operator <see cref="operatorId"/> is ready to join the
        /// group communication topology.
        /// </summary>
        /// <param name="taskId">The current task</param>
        /// <param name="operatorId">The identifier of the operator ready to join the topology</param>
        public void JoinTopology(string taskId, string stageName, int operatorId)
        {
            int offset = 0;
            byte[] message = new byte[sizeof(ushort) + stageName.Length + sizeof(ushort) + sizeof(ushort)];
            Buffer.BlockCopy(BitConverter.GetBytes(stageName.Length), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(ByteUtilities.StringToByteArrays(stageName), 0, message, offset, stageName.Length);
            offset += stageName.Length;
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.JoinTopology), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)operatorId), 0, message, offset, sizeof(ushort));

            LOGGER.Log(Level.Info, $"Operator {operatorId} requesting to join the topology through heartbeat.");

            Send(taskId, message);
        }

        /// <summary>
        /// Send a notification to the driver for an update on topology state.
        /// </summary>
        /// <param name="taskId">The current task id</param>
        /// <param name="operatorId">The operator requiring the topology update</param>
        public void TopologyUpdateRequest(string taskId, string stageName, int operatorId)
        {
            int offset = 0;
            byte[] message = new byte[sizeof(ushort) + stageName.Length + sizeof(ushort) + sizeof(ushort)];
            Buffer.BlockCopy(BitConverter.GetBytes(stageName.Length), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(ByteUtilities.StringToByteArrays(stageName), 0, message, offset, stageName.Length);
            offset += stageName.Length;
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.TopologyUpdateRequest), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)operatorId), 0, message, offset, sizeof(ushort));
     
            LOGGER.Log(Level.Info, string.Format("Operator {0} requesting a topology update through heartbeat", operatorId));

            Send(taskId, message);
        }

        /// <summary>
        /// Notify the driver that the current task is ready to accept new incoming data.
        /// </summary>
        /// <param name="taskId">The current task id</param>
        /// <param name="iteration">The current iteration number</param>
        public void NextDataRequest(string taskId, string stageName, int iteration)
        {
            int offset = 0;
            byte[] message = new byte[sizeof(ushort) + stageName.Length + sizeof(ushort) + sizeof(ushort)];
            Buffer.BlockCopy(BitConverter.GetBytes(stageName.Length), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(ByteUtilities.StringToByteArrays(stageName), 0, message, offset, stageName.Length);
            offset += stageName.Length;
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.NextDataRequest), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)iteration), 0, message, offset, sizeof(ushort));

            LOGGER.Log(Level.Info, "Sending request for data through heartbeat.");

            Send(taskId, message);     
        }

        /// <summary>
        /// Signal the driver that the current stage is completed.
        /// </summary>
        /// <param name="taskId">The current task identifier</param>
        public void StageComplete(string taskId, string stageName)
        {
            int offset = 0;
            byte[] message = new byte[sizeof(ushort) + stageName.Length + sizeof(ushort)];
            Buffer.BlockCopy(BitConverter.GetBytes(stageName.Length), 0, message, offset, sizeof(ushort));
            offset += sizeof(ushort);
            Buffer.BlockCopy(ByteUtilities.StringToByteArrays(stageName), 0, message, offset, stageName.Length);
            offset += stageName.Length;
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.CompleteStage), 0, message, offset, sizeof(ushort));

            LOGGER.Log(Level.Info, "Sending notification that the stage is completed.");

            Send(taskId, message);
        }
    }
}
