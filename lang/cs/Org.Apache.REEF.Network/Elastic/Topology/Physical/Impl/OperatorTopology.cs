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

using Org.Apache.REEF.Network.Elastic.Task.Impl;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal abstract class OperatorTopology : IObserver<NsMessage<GroupCommunicationMessage>>, IWaitForTaskRegistration, IDisposable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(OperatorTopology));

        protected readonly ConcurrentDictionary<int, string> _children = new ConcurrentDictionary<int, string>();
        protected string _rootTaskId;
        protected readonly string _taskId;
        protected bool _initialized;
        internal CommunicationLayer _commLayer;

        private readonly int _timeout;

        protected ConcurrentQueue<GroupCommunicationMessage> _sendQueue;

        protected BlockingCollection<GroupCommunicationMessage> _messageQueue;

        internal OperatorTopology(string taskId, int rootId, string subscription, int operatorId, CommunicationLayer commLayer,
            int timeout)
        {
            _taskId = taskId;
            SubscriptionName = subscription;
            OperatorId = operatorId;
            _initialized = false;
            _commLayer = commLayer;

            _messageQueue = new BlockingCollection<GroupCommunicationMessage>();
            _sendQueue = new ConcurrentQueue<GroupCommunicationMessage>();

            if (rootId >= 0)
            {
                _rootTaskId = Utils.BuildTaskId(SubscriptionName, rootId);
            }

            _timeout = timeout;
        }

        internal string SubscriptionName { get; set; }

        internal int OperatorId { get; set; }

        internal IEnumerator<GroupCommunicationMessage> Receive(CancellationTokenSource cancellationSource)
        {
             return _messageQueue.GetConsumingEnumerable(cancellationSource.Token).GetEnumerator();
        }

        internal virtual void Send(List<GroupCommunicationMessage> messages, CancellationTokenSource cancellationSource)
        {
            foreach (var message in messages)
            {
                _sendQueue.Enqueue(message);
            }

            if (_initialized)
            {
                Send(cancellationSource);
            }
        }

        public virtual void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
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

            Send(cancellationSource);
        }

        public virtual void OnNext(NsMessage<GroupCommunicationMessage> message)
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

                if (_children.Count > 0)
                {
                    _sendQueue.Enqueue(payload);
                }
            }

            if (_initialized)
            {
                Send(new CancellationTokenSource());
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
            var elapsedTime = 0;
            while (_sendQueue.Count > 0 && elapsedTime < _timeout)
            {
                // The topology is still trying to send messages, wait
                Thread.Sleep(100);
                elapsedTime += 100;
            }

            _commLayer.Dispose();
        }

        protected virtual void Send(CancellationTokenSource cancellationSource)
        {
            while (_sendQueue.Count > 0)
            {
                GroupCommunicationMessage message;
                _sendQueue.TryPeek(out message);

                foreach (var child in _children.Values)
                {
                    _commLayer.Send(child, message);
                }
                _sendQueue.TryDequeue(out message);
            }
        }
    }
}
