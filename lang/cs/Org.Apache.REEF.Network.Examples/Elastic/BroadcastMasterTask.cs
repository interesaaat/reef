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

using System;
using System.Diagnostics;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Examples.GroupCommunication;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public class BroadcastMasterTask : ITask
    {
        private readonly IGroupCommClient _groupCommClient;
        private readonly ICommunicationGroupClient _commGroup;
        private readonly IBroadcastSender<int> _broadcastSender;

        [Inject]
        public BroadcastMasterTask(
            IGroupCommClient groupCommClient)
        {
            _groupCommClient = groupCommClient;

            _commGroup = groupCommClient.GetCommunicationGroup(GroupTestConstants.GroupName);
            _broadcastSender = _commGroup.GetBroadcastSender<int>(GroupTestConstants.BroadcastOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            _groupCommClient.Initialize();

            var number = new Random().Next();

            _broadcastSender.Send(number);

            Console.WriteLine("Master has sent {0}", number);

            return null;
        }

        public void Dispose()
        {
            _groupCommClient.Dispose();

            Console.WriteLine("Disposed.");
        }
    }
}
