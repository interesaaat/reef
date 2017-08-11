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
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using System.Runtime.Remoting;
using System.Linq;
using Org.Apache.REEF.Network.NetworkService;

namespace Org.Apache.REEF.Network.Elastic.Topology.Task.Impl
{
    public class OperatorTopology : IObserver<NsMessage<GroupCommunicationMessage>>, IDisposable
    {
        protected readonly ConcurrentDictionary<int, string> _children = new ConcurrentDictionary<int, string>();
        protected int _rootId;
        protected string _taskId;
        internal CommunicationLayer _commLayer;

        protected BlockingCollection<GroupCommunicationMessage> _messageQueue;

        protected OperatorTopology()
        {
        }

        public string SubscriptionName { get; protected set; }

        public int OperatorId { get; protected set; }

        public IEnumerator<GroupCommunicationMessage> Receive(CancellationTokenSource cancellationSource)
        {
             return _messageQueue.GetConsumingEnumerable(cancellationSource.Token).GetEnumerator();
        }

        public void Send(GroupCommunicationMessage[] messages)
        {
            foreach (var child in _children.Values)
            {
                foreach (var message in messages)
                {
                    _commLayer.Send(child, message);
                }
            }  
        }

        public void WaitingForRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                _commLayer.WaitForTaskRegistration(_children.Values.ToList(), cancellationSource);
            }
            catch (RemotingException e)
            {
                throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + _taskId, e);
            }
        }

        public void OnNext(NsMessage<GroupCommunicationMessage> message)
        {
            if (_messageQueue.IsAddingCompleted)
            {
                _messageQueue = new BlockingCollection<GroupCommunicationMessage>();
            }

            foreach (var payload in message.Data)
            {
                _messageQueue.Add(payload);
            }
        }

        public void OnError(Exception error)
        {
            _messageQueue.CompleteAdding();
        }

        public void OnCompleted()
        {
            _messageQueue.CompleteAdding();
        }

        public void Dispose()
        {
            _commLayer.Dispose();
        }
    }
}
