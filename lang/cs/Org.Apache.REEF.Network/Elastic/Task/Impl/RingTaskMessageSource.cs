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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using System;
using Org.Apache.REEF.Common.Runtime.Evaluator;
using Org.Apache.REEF.Network.Elastic.Comm;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    public class RingTaskMessageSource : ITaskMessageSource
    {
        private string _taskId;
        private string _taskIdWithToken;

        private readonly HeartBeatReference _heartBeatManager;

        private readonly object _lock;

        private byte[] _message;

        [Inject]
        private RingTaskMessageSource(HeartBeatReference heartBeatManager)
        {
            _heartBeatManager = heartBeatManager;

            _taskId = string.Empty;
            _taskIdWithToken = string.Empty;
            _message = null;

            _lock = new object();
        }

        public void IterationNumber(string taskId, int iteration)
        {
            lock (_lock)
            {
                _taskId = taskId;
                _message = new byte[6];
                Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.IterationNumber), 0, _message, 0, sizeof(ushort));
                Buffer.BlockCopy(BitConverter.GetBytes(iteration), 0, _message, sizeof(ushort), sizeof(int));

                _heartBeatManager.Heartbeat();
            }
        }

        public void JoinTheRing(string taskId, int iteration)
        {
            lock (_lock)
            {
                _taskId = taskId;
                _message = new byte[6];
                Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.JoinTheRing), 0, _message, 0, sizeof(ushort));
                Buffer.BlockCopy(BitConverter.GetBytes(iteration), 0, _message, sizeof(ushort), sizeof(int));
                _heartBeatManager.Heartbeat();
            }
        }

        public void TokenResponse(string taskId, bool response)
        {
            lock (_lock)
            {
                _taskId = taskId;
                _message = new byte[3];
                Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.TokenResponse), 0, _message, 0, sizeof(ushort));
                _message[2] = response ? (byte)1 : (byte)0;

                _heartBeatManager.Heartbeat();
            }
        }

        internal void NextTokenRequest(string taskId, int iteration)
        {
            lock (_lock)
            {
                _taskId = taskId;
                _message = new byte[6];
                Buffer.BlockCopy(BitConverter.GetBytes((ushort)TaskMessageType.NextTokenRequest), 0, _message, 0, sizeof(ushort));
                Buffer.BlockCopy(BitConverter.GetBytes(iteration), 0, _message, sizeof(ushort), sizeof(int));
                _heartBeatManager.Heartbeat();
            }
        }

        public Optional<TaskMessage> Message
        {
            get
            {
                if (_message != null)
                {
                    var message = TaskMessage.From(_taskId, _message);
                    _message = null;

                    return Optional<TaskMessage>.Of(message);
                }

                return Optional<TaskMessage>.Empty();
            }
        }
    }
}
