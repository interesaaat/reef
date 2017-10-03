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

        private readonly byte[] _message1 = BitConverter.GetBytes((ushort)TaskMessageType.JoinTheRing);
        ////private readonly byte[] _message2 = BitConverter.GetBytes((ushort)TaskMessageType.TokenReceived);

        [Inject]
        private RingTaskMessageSource(HeartBeatReference heartBeatManager)
        {
            _heartBeatManager = heartBeatManager;

            _taskId = string.Empty;
            _taskIdWithToken = string.Empty;
        }

        public void JoinTheRing(string taskId)
        {
            _taskId = taskId;

            _heartBeatManager.Heartbeat();
        }

        public Optional<TaskMessage> Message
        {
            get
            {
                if (_taskId != string.Empty)
                {
                    var message = TaskMessage.From(_taskId, _message1);
                    _taskId = string.Empty;

                    return Optional<TaskMessage>.Of(message);
                }
                ////if (_taskIdWithToken != string.Empty)
                ////{
                ////    List<byte[]> buffer = new List<byte[]>(2)
                ////    {
                ////        _message2,
                ////        BitConverter.GetBytes(_iterationNumber)
                ////    };
                ////    var message = TaskMessage.From(_taskIdWithToken, buffer.SelectMany(i => i).ToArray());
                ////    _taskIdWithToken = string.Empty;

                ////    return Optional<TaskMessage>.Of(message);
                ////}

                return Optional<TaskMessage>.Empty();
            }
        }
    }
}
