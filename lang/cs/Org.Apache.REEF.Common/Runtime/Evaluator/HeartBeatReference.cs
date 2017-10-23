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

using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    public class HeartBeatReference
    {
        private readonly IHeartBeatManager _heartBeatManager;

        [Inject]
        internal HeartBeatReference(IInjector subInjector)
        {
            _heartBeatManager = subInjector.GetInstance<IHeartBeatManager>();
        }

        public void Heartbeat(TaskStatusProto proto)
        {
            var state = _heartBeatManager.ContextManager.GetTaskState();

            if (state.IsPresent())
            {
                proto.state = state.Value;
            }

            _heartBeatManager.OnNext(proto);
        }

        public void Heartbeat()
        {
            _heartBeatManager.OnNext();
        }
    }
}
