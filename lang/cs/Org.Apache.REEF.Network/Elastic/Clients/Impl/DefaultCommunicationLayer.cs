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

using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Clients.Impl
{
    public class DefaultCommunicationLayer : ICommunicationLayer
    {
        private readonly int _timeout;
        private readonly int _retryCount;
        private readonly int _sleepTime;

        ////private readonly ChildNodeContainer<T> _childNodeContainer = new ChildNodeContainer<T>();
        ////private readonly NodeStruct<T> _parent;
        ////private readonly INameClient _nameClient;
        ////private readonly Sender _sender;

        [Inject]
        private DefaultCommunicationLayer(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.RetryCountWaitingForRegistration))] int retryCount,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SleepTimeWaitingForRegistration))] int sleepTime,
            NetworkLayer network)
        {
            _timeout = timeout;
            _retryCount = retryCount;
            _sleepTime = sleepTime;
        }

        public IEnumerator<object> Receive(CancellationTokenSource cancellationSource)
        {
            throw new NotImplementedException();
        }

        public void Send(object[] data)
        {
            throw new NotImplementedException();
        }

        public void WaitingForRegistration(CancellationTokenSource cancellationSource)
        {
            throw new NotImplementedException();
        }
    }
}
