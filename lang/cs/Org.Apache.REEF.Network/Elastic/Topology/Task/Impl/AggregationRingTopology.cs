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
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Exceptions;
using System.Diagnostics;

namespace Org.Apache.REEF.Network.Elastic.Topology.Task.Impl
{
    public class AggregationRingTopology : DriverAwareOperatorTopology
    {
        private BlockingCollection<string> _next;

        [Inject]
        private AggregationRingTopology(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscription,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(OperatorsConfiguration.OperatorId))] int operatorId,
            CommunicationLayer commLayer) : base(taskId, rootId, subscription)
        {
            OperatorId = operatorId;
            _commLayer = commLayer;
            _next = new BlockingCollection<string>();

            _messageQueue = new BlockingCollection<GroupCommunicationMessage>();

            foreach (var child in children)
            {
                var childTaskId = Utils.BuildTaskId(SubscriptionName, child);

                _children.TryAdd(child, childTaskId);

                _commLayer.RegisterOperatorTopologyForTask(childTaskId, this);
            }

            _commLayer.RegisterOperatorTopologyForDriver(_taskId, this);
        }

        public override void OnNext(NsMessage<GroupCommunicationMessage> message)
        {
            if (_messageQueue.IsAddingCompleted)
            {
                if (_messageQueue.Count > 0)
                {
                    throw new IllegalStateException("Trying to add messages to a closed non-empty queue");
                }
                _messageQueue = new BlockingCollection<GroupCommunicationMessage>();
            }

            foreach (var payload in message.Data)
            {
                _messageQueue.Add(payload);
            }
        }

        public override void OnNext(string value)
        {
            _next.Add(value);
        }

        public void WaitForToken()
        {
            if (_taskId != _rootId)
            {
                _commLayer.WaitingForToken(_taskId);
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            while (_sendQueue.Count > 0)
            {
                GroupCommunicationMessage message;
                _sendQueue.TryPeek(out message);

                var next = _next.Take(cancellationSource.Token);

                _commLayer.Send(next, message);
                _sendQueue.TryDequeue(out message);
            }
        }
    }
}
