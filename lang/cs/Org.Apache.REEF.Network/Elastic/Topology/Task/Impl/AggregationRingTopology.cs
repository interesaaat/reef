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

using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Topology.Task.Impl
{
    public class AggregationRingTopology : DriverAwareOperatorTopology
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(OperatorTopology));

        private BlockingCollection<string> _next;

        [Inject]
        private AggregationRingTopology(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscription,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(OperatorsConfiguration.OperatorId))] int operatorId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int timeout,
            CommunicationLayer commLayer) : base(taskId, rootId, subscription, timeout, operatorId, commLayer)
        {
            _next = new BlockingCollection<string>();

            foreach (var child in children)
            {
                var childTaskId = Utils.BuildTaskId(SubscriptionName, child);

                _children.TryAdd(child, childTaskId);

               _commLayer.RegisterOperatorTopologyForTask(childTaskId, this);
            }

            _commLayer.RegisterOperatorTopologyForDriver(_taskId, this);
        }

        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                _commLayer.WaitForTaskRegistration(_children.Values.ToList(), cancellationSource);
            }
            catch (Exception e)
            {
                throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + _taskId, e);
            }

            _initialized = true;
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
            if (_taskId != _rootTaskId)
            {
                _commLayer.WaitingForToken(_taskId);
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            _sendQueue.TryPeek(out message);

            var nextNode = _next.Take();

            _commLayer.Send(nextNode, message);
            _sendQueue.TryDequeue(out message);
        }
    }
}
