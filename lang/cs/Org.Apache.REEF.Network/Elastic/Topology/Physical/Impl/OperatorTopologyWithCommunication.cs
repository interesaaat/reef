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
using System.Threading;
using System.Linq;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal abstract class OperatorTopologyWithCommunication : DriverAwareOperatorTopology, IObserver<NsMessage<GroupCommunicationMessage>>, IWaitForTaskRegistration, IDisposable
    {
        protected static readonly Logger Logger = Logger.GetLogger(typeof(OperatorTopologyWithCommunication));

        protected readonly ConcurrentDictionary<int, string> _children = new ConcurrentDictionary<int, string>();
        protected bool _initialized;
        internal CommunicationLayer _commLayer;

        protected readonly int _disposeTimeout;
        protected readonly int _timeout;
        protected readonly int _retry;

        protected ConcurrentQueue<GroupCommunicationMessage> _sendQueue;

        protected BlockingCollection<GroupCommunicationMessage> _messageQueue;

        internal OperatorTopologyWithCommunication(
            string taskId, 
            int rootId, 
            string subscription, 
            int operatorId, 
            CommunicationLayer commLayer, 
            int retry, 
            int timeout, 
            int disposeTimeout) : base(taskId, rootId, subscription, operatorId)
        {
            _initialized = false;
            _commLayer = commLayer;

            _messageQueue = new BlockingCollection<GroupCommunicationMessage>();
            _sendQueue = new ConcurrentQueue<GroupCommunicationMessage>();

            _retry = retry;
            _timeout = timeout;
            _disposeTimeout = disposeTimeout;
        }

        internal virtual GroupCommunicationMessage Receive(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            int retry = 1;

            while (!_messageQueue.TryTake(out message, _timeout, cancellationSource.Token))
            {
                if (cancellationSource.IsCancellationRequested)
                {
                    throw new OperationCanceledException("Received cancellation request: stop receiving");
                }

                _commLayer.NextDataRequest(_taskId, -1);
                if (retry++ > _retry)
                {
                    throw new Exception(string.Format(
                        "Failed to receive message in the ring after {0} try", _retry));
                }
            }

            return message;
        }

        internal virtual void Send(GroupCommunicationMessage message, CancellationTokenSource cancellationSource)
        {
            _sendQueue.Enqueue(message);

            if (_initialized)
            {
                Send(cancellationSource);
            }
        }

        internal virtual void JoinTopology()
        {
            _commLayer.JoinTopology(_taskId, OperatorId);
        }

        internal void SignalSubscriptionComplete()
        {
            if (_taskId == _rootTaskId)
            {
                _commLayer.SignalSubscriptionComplete(_taskId);
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

            _messageQueue.Add(message.Data);

            if (!_children.IsEmpty)
            {
                _sendQueue.Enqueue(message.Data);
            }

            if (_initialized)
            {
                Send(new CancellationTokenSource());
            }
        }

        public new void OnError(Exception error)
        {
            _messageQueue.CompleteAdding();
        }

        public new void OnCompleted()
        {
            _messageQueue.CompleteAdding();
        }

        public override void WaitCompletionBeforeDisposing()
        {
            var elapsedTime = 0;
            while (_sendQueue.Count > 0  && elapsedTime < _disposeTimeout)
            {
                // The topology is still trying to send messages, wait
                Thread.Sleep(100);
                elapsedTime += 100;
            }
        }

        public virtual void Dispose()
        {
            _messageQueue.CompleteAdding();

            _commLayer.Dispose();
        }

        protected virtual void Send(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            while (_sendQueue.TryDequeue(out message) && !cancellationSource.IsCancellationRequested)
            {
                foreach (var child in _children.Values)
                {
                    _commLayer.Send(child, message, cancellationSource);
                }
            }
        }
    }
}
