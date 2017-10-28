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

using Org.Apache.REEF.Tang.Annotations;
using System;
using Org.Apache.REEF.Common.Runtime.Evaluator;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    public class RingTaskMessageSource
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(CommunicationLayer));

        private readonly HeartBeatReference _heartBeatManager;

        [Inject]
        private RingTaskMessageSource(HeartBeatReference heartBeatManager)
        {
            _heartBeatManager = heartBeatManager;
        }

        internal void IterationNumber(string taskId, int iteration)
        {
            byte[] message = new byte[6];
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.IterationNumber), 0, message, 0, sizeof(ushort));
            Buffer.BlockCopy(BitConverter.GetBytes(iteration), 0, message, sizeof(ushort), sizeof(int));

            Logger.Log(Level.Info, string.Format("Sending current iteration number ({0}) through heartbeat", iteration));

            Send(taskId, message);
        }

        internal void JoinTheRing(string taskId, int iteration)
        {
            var message = new byte[6];
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.JoinTheRing), 0, message, 0, sizeof(ushort));
            Buffer.BlockCopy(BitConverter.GetBytes(iteration), 0, message, sizeof(ushort), sizeof(int));

            Logger.Log(Level.Info, "Going to request to join the ring through heartbeat");

            Send(taskId, message);
        }

        internal void TokenResponse(string taskId, int iteration, bool response)
        {
            var message = new byte[7];
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.TokenResponse), 0, message, 0, sizeof(ushort));
            Buffer.BlockCopy(BitConverter.GetBytes(iteration), 0, message, sizeof(ushort), sizeof(int));
            message[6] = response ? (byte)1 : (byte)0;

            Logger.Log(Level.Info, string.Format("Sending response message ({0} for iteration {1}) through heartbeat", response, iteration));

            Send(taskId, message);
        }

        internal void NextTokenRequest(string taskId, int iteration)
        {
            var message = new byte[6];
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.NextTokenRequest), 0, message, 0, sizeof(ushort));
            Buffer.BlockCopy(BitConverter.GetBytes(iteration), 0, message, sizeof(ushort), sizeof(int));

            Logger.Log(Level.Info, string.Format("Sending request for next node in the ring at iteration {0} through heartbeat", iteration));

            Send(taskId, message);
        }

        internal void NextDataRequest(string taskId, int iteration)
        {
            var message = new byte[6];
            Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.NextDataRequest), 0, message, 0, sizeof(ushort));
            Buffer.BlockCopy(BitConverter.GetBytes(iteration), 0, message, sizeof(ushort), sizeof(int));

            Logger.Log(Level.Info, "Sending request for data through heartbeat");

            Logger.Log(Level.Info, "Sending request for data through heartbeat");

            Send(taskId, message);     
        }

        private void Send(string taskId, byte[] message)
        {
            TaskStatusProto taskStatusProto = new TaskStatusProto()
            {
                task_id = taskId,
                context_id = Utils.GetContextIdFromTaskId(taskId)
            };

            TaskStatusProto.TaskMessageProto taskMessageProto = new TaskStatusProto.TaskMessageProto()
            {
                source_id = taskId,
                message = message,
            };
            taskStatusProto.task_message.Add(taskMessageProto);

            _heartBeatManager.Heartbeat(taskStatusProto);
        }
    }
}
