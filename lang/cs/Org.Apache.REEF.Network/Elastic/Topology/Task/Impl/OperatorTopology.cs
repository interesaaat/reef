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
    public class OperatorTopology : IOperatorTopology, IObserver<NsMessage<GroupCommunicationMessage>>
    {
        private readonly ConcurrentDictionary<int, string> _children = new ConcurrentDictionary<int, string>();
        private int _rootId;
        private string _taskId;
        CommunicationLayer _commLayer;

        private BlockingCollection<object> _messageQueue;

        [Inject]
        private OperatorTopology(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscription,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(OperatorsConfiguration.OperatorId))] int operatorId,
            CommunicationLayer commLayer)
        {
            SubscriptionName = subscription;
            _rootId = rootId;
            _taskId = taskId;
            OperatorId = operatorId;
            _commLayer = commLayer;
            _messageQueue = new BlockingCollection<object>();

            if (_taskId != Utils.BuildTaskId(SubscriptionName, _rootId)) 
            {
                _commLayer.RegisterOperatorTopologyForTask(Utils.BuildTaskId(SubscriptionName, _rootId), this);
            }

            foreach (var child in children)
            {
                var childTaskId = Utils.BuildTaskId(SubscriptionName, child);

                _children.TryAdd(child, childTaskId);
                _commLayer.RegisterOperatorTopologyForTask(childTaskId, this);
            }
        }

        public string SubscriptionName { get; private set; }

        public int OperatorId { get; private set; }

        public IEnumerator<object> Receive(CancellationTokenSource cancellationSource)
        {
            return _messageQueue.GetConsumingEnumerable(cancellationSource.Token).GetEnumerator();
        }

        public void Send(object[] data)
        {
            var message = new DataMessage(SubscriptionName, OperatorId);

            foreach (var child in _children.Values)
            {
                foreach (var datum in data)
                {
                    message.Data = datum;

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
            foreach (var data in message.Data)
            {
                if (data.GetType() == typeof(DataMessage))
                {
                    var dataMessage = data as DataMessage;

                    if (dataMessage.Data != null)
                    {
                        _messageQueue.Add(dataMessage.Data);
                    }
                }
            }
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
