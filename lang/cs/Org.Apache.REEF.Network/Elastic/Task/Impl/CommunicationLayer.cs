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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Utilities.Logging;
using System.Threading;
using System.Runtime.Remoting;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Handles all incoming messages for this Task.
    /// Writable version
    /// </summary>
    internal sealed class CommunicationLayer : 
        IObserver<IRemoteMessage<NsMessage<GroupCommunicationMessage>>>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(CommunicationLayer));

        private readonly int _timeout;
        private readonly int _retryCount;
        private readonly int _sleepTime;
        private readonly StreamingNetworkService<GroupCommunicationMessage> _networkService;
        private readonly RingTaskMessageSource _ringMessageSource;
        private readonly DriverMessageHandler _driverMessagesHandler;
        private readonly IIdentifierFactory _idFactory;

        private bool _disposed;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<NodeObserverIdentifier, OperatorTopology>> _messageObservers =
            new ConcurrentDictionary<string, ConcurrentDictionary<NodeObserverIdentifier, OperatorTopology>>();

        private readonly ConcurrentDictionary<IIdentifier, IConnection<GroupCommunicationMessage>> _registeredConnections = 
            new ConcurrentDictionary<IIdentifier, IConnection<GroupCommunicationMessage>>();

        /// <summary>
        /// Creates a new GroupCommNetworkObserver.
        /// </summary>
        [Inject]
        private CommunicationLayer(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.RetryCountWaitingForRegistration))] int retryCount,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SleepTimeWaitingForRegistration))] int sleepTime,
            StreamingNetworkService<GroupCommunicationMessage> networkService,
            RingTaskMessageSource ringMessageSource,
            DriverMessageHandler driverMessagesHandler,
            IIdentifierFactory idFactory)
        {
            _timeout = timeout;
            _retryCount = retryCount;
            _sleepTime = sleepTime;
            _networkService = networkService;
            _ringMessageSource = ringMessageSource;
            _driverMessagesHandler = driverMessagesHandler;
            _idFactory = idFactory;

            _disposed = false;

            _networkService.RemoteManager.RegisterObserver(this);
        }

        /// <summary>
        /// Registers a <see cref="OperatorTopology"/> for a given <see cref="taskSourceId"/>.
        /// If the <see cref="OperatorTopology"/> has already been initialized, it will return
        /// the existing one.
        /// </summary>
        public void RegisterOperatorTopologyForTask(string taskSourceId, OperatorTopology operatorObserver)
        {
            // Add a TaskMessage observer for each upstream/downstream source.
            ConcurrentDictionary<NodeObserverIdentifier, OperatorTopology> taskObservers;
            var id = NodeObserverIdentifier.FromObserver(operatorObserver);

            _messageObservers.TryGetValue(taskSourceId, out taskObservers);

            if (taskObservers == null)
            {
                taskObservers = new ConcurrentDictionary<NodeObserverIdentifier, OperatorTopology>();
                _messageObservers.TryAdd(taskSourceId, taskObservers);
            }

            if (taskObservers.ContainsKey(id))
            {
                throw new IllegalStateException("Topology for id " + id + " already added among listeners");
            }

            taskObservers.TryAdd(id, operatorObserver);
        }

        public void RegisterOperatorTopologyForDriver(string taskDestinationId, DriverAwareOperatorTopology operatorObserver)
        {
            _driverMessagesHandler.RegisterOperatorTopologyForDriver(taskDestinationId, operatorObserver);
        }

        /// <summary>
        /// Send the GroupCommunicationMessage to the Task whose name is
        /// included in the message.
        /// </summary>
        /// <param name="message">The message to send.</param>
        internal void Send(string destination, GroupCommunicationMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }
            if (string.IsNullOrEmpty(destination))
            {
                throw new ArgumentException("Message destination cannot be null or empty");
            }

            IIdentifier destId = _idFactory.Create(destination);

            var conn = _registeredConnections.GetOrAdd(destId, _networkService.NewConnection(destId));

            if (!conn.IsOpen)
            {
                conn.Open();
            }

            conn.Write(message);
        }

        /// <summary>
        /// Forward the received message to the target <see cref="OperatorTopology"/>.
        /// </summary>
        /// <param name="remoteMessage"></param>
        public void OnNext(IRemoteMessage<NsMessage<GroupCommunicationMessage>> remoteMessage)
        {
            var nsMessage = remoteMessage.Message;
            var gcm = nsMessage.Data.First();
            var gcMessageTaskSource = nsMessage.SourceId.ToString();
            var id = NodeObserverIdentifier.FromMessage(gcm);
            OperatorTopology operatorObserver;

            if (!_messageObservers.TryGetValue(gcMessageTaskSource, out ConcurrentDictionary<NodeObserverIdentifier, OperatorTopology> observers))
            {
                throw new KeyNotFoundException("Unable to find registered NodeMessageObserver for source Task " +
                    gcMessageTaskSource + ".");
            }

            if (!observers.TryGetValue(id, out operatorObserver))
            {
                throw new KeyNotFoundException("Unable to find registered Operator Topology for Subscription " +
                    gcm.SubscriptionName + " operator " + gcm.OperatorId);
            }

            operatorObserver.OnNext(nsMessage);
        }

        public void JoinTheRing(string taskId)
        {
            _ringMessageSource.JoinTheRing(taskId);
        }

        public void TokenReceived(string taskId, int iterationNumber)
        {
            _ringMessageSource.TokenReceived(taskId, iterationNumber);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
            foreach (var observers in _messageObservers.Values)
            {
                foreach (var observer in observers.Values)
                {
                    observer.OnCompleted();
                }
            }

            _messageObservers.Clear();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                foreach (var conn in _registeredConnections.Values)
                {
                    if (conn != null && conn.IsOpen)
                    {
                        conn.Dispose();
                    }
                }

                foreach (var observers in _messageObservers.Values)
                {
                    foreach (var observer in observers.Values)
                    {
                        observer.OnCompleted();
                    }
                }

                _disposed = true;

                Logger.Log(Level.Info, "Disposed of group communication layer");
            }
        }

        internal bool IsAlive(string nodeIdentifier, CancellationTokenSource cancellationSource)
        {
            return _networkService.NamingClient.Lookup(nodeIdentifier) != null;
        }

        /// <summary>
        /// Checks if the identifier is registered with the Name Server.
        /// Throws exception if the operation fails more than the retry count.
        /// </summary>
        /// <param name="identifiers">The identifier to look up</param>
        /// <param name="cancellationSource">The token to cancel the operation</param>
        internal void WaitForTaskRegistration(IList<string> identifiers, CancellationTokenSource cancellationSource)
        {
            using (Logger.LogFunction("CommunicationLayer::WaitForTaskRegistration"))
            {
                IList<string> foundList = new List<string>();
                for (var i = 0; i < _retryCount; i++)
                {
                    if (cancellationSource != null && cancellationSource.Token.IsCancellationRequested)
                    {
                        Logger.Log(Level.Info, "OperatorTopology.WaitForTaskRegistration is canceled in retryCount {0}.", i);
                        throw new OperationCanceledException("WaitForTaskRegistration is canceled");
                    }

                    Logger.Log(Level.Info, "OperatorTopology.WaitForTaskRegistration, in retryCount {0}.", i);
                    foreach (var identifier in identifiers)
                    {
                        if (!foundList.Contains(identifier) && Lookup(identifier))
                        {
                            foundList.Add(identifier);
                            Logger.Log(Level.Verbose, "OperatorTopology.WaitForTaskRegistration, find a dependent id {0} at loop {1}.", identifier, i);
                        }
                    }

                    if (foundList.Count == identifiers.Count)
                    {
                        Logger.Log(Level.Info, "OperatorTopology.WaitForTaskRegistration, found all {0} dependent ids at loop {1}.", foundList.Count, i);
                        return;
                    }

                    Thread.Sleep(_sleepTime);
                }

                var leftOver = foundList.Count == 0 ? string.Join(",", identifiers) : string.Join(",", identifiers.Where(e => !foundList.Contains(e)));
                Logger.Log(Level.Error, "Cannot find registered parent/children: {0}.", leftOver);
                throw new RemotingException("Failed to find parent/children nodes");
            }
        }

        internal bool Lookup(string identifier)
        {
            if (_disposed || _networkService == null)
            {
                return false;
            }
            return _networkService.NamingClient.Lookup(identifier) != null;
        }
    }
}