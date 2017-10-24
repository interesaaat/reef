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
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;

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
        private readonly CheckpointService _checkpointService;

        private bool _disposed;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<NodeObserverIdentifier, OperatorTopologyWithCommunication>> _groupMessageObservers =
            new ConcurrentDictionary<string, ConcurrentDictionary<NodeObserverIdentifier, OperatorTopologyWithCommunication>>();

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology>> _driverMessageObservers =
             new ConcurrentDictionary<string, ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology>>();

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
            CheckpointService checkpointService,
            IIdentifierFactory idFactory)
        {
            _timeout = timeout;
            _retryCount = retryCount;
            _sleepTime = sleepTime;
            _networkService = networkService;
            _ringMessageSource = ringMessageSource;
            _driverMessagesHandler = driverMessagesHandler;
            _checkpointService = checkpointService;
            _checkpointService.CommunicationLayer = this;
            _idFactory = idFactory;

            _disposed = false;

            _networkService.RemoteManager.RegisterObserver(this);
            _driverMessagesHandler.DriverMessageObservers = _driverMessageObservers;
        }

        /// <summary>
        /// Registers a <see cref="OperatorTopologyWithCommunication"/> for a given <see cref="taskDestinationId"/>.
        /// If the <see cref="OperatorTopologyWithCommunication"/> has already been initialized, it will return
        /// the existing one.
        /// </summary>
        public void RegisterOperatorTopologyForTask(string taskDestinationId, OperatorTopologyWithCommunication operatorObserver)
        {
            ConcurrentDictionary<NodeObserverIdentifier, OperatorTopologyWithCommunication> taskObservers;
            var id = NodeObserverIdentifier.FromObserver(operatorObserver);

            if (!_groupMessageObservers.TryGetValue(taskDestinationId, out taskObservers))
            {
                taskObservers = new ConcurrentDictionary<NodeObserverIdentifier, OperatorTopologyWithCommunication>();
                _groupMessageObservers.TryAdd(taskDestinationId, taskObservers);
            }

            if (taskObservers.ContainsKey(id))
            {
                throw new IllegalStateException("Topology for id " + id + " already added among listeners");
            }

            taskObservers.TryAdd(id, operatorObserver);
        }

        internal void RegisterOperatorTopologyForDriver(string taskDestinationId, DriverAwareOperatorTopology operatorObserver)
        {
            // Add a TaskMessage observer for each upstream/downstream source.
            ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology> taskObservers;
            var id = NodeObserverIdentifier.FromObserver(operatorObserver);

            _driverMessageObservers.TryGetValue(taskDestinationId, out taskObservers);

            if (taskObservers == null)
            {
                taskObservers = new ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology>();
                _driverMessageObservers.TryAdd(taskDestinationId, taskObservers);
            }

            if (taskObservers.ContainsKey(id))
            {
                throw new IllegalStateException("Topology for id " + id + " already added among driver listeners");
            }

            taskObservers.TryAdd(id, operatorObserver);
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
            if (_disposed)
            {
                Logger.Log(Level.Warning, "Received send message request after disposing: Ignoring");
                return;
            }

            IIdentifier destId = _idFactory.Create(destination);

            var conn = _registeredConnections.GetOrAdd(destId, _networkService.NewConnection(destId));

            if (!conn.IsOpen)
            {
                conn.Open();
            }

            Console.WriteLine("Sending to node " + destination);

            conn.Write(message);
        }

        /// <summary>
        /// Forward the received message to the target <see cref="OperatorTopologyWithCommunication"/>.
        /// </summary>
        /// <param name="remoteMessage"></param>
        public void OnNext(IRemoteMessage<NsMessage<GroupCommunicationMessage>> remoteMessage)
        {
            if (_disposed)
            {
                Logger.Log(Level.Warning, "Received message after disposing: Ignoring");
                return;
            }

            var nsMessage = remoteMessage.Message;
            var gcm = nsMessage.Data.First();
            var gcMessageTaskSource = nsMessage.SourceId.ToString();

            if (gcm.GetType() == typeof(CheckpointMessageRequest))
            {
                var cpm = gcm as CheckpointMessageRequest;
                ICheckpointState checkpoint;
                if (_checkpointService.GetCheckpoint(out checkpoint, nsMessage.DestId.ToString(), cpm.SubscriptionName, cpm.OperatorId, cpm.Iteration))
                {
                    var returnMessage = checkpoint.ToMessage();

                    returnMessage.Payload = checkpoint;

                    Send(gcMessageTaskSource, returnMessage);
                }

                return;
            }
            if (gcm.GetType() == typeof(CheckpointMessage))
            {
                Logger.Log(Level.Info, "Received checkpoint from " + gcMessageTaskSource);
                var cpm = gcm as CheckpointMessage;
                cpm.Payload.TaskId = nsMessage.DestId.ToString();
                _checkpointService.Checkpoint(cpm.Payload);
                return;
            }
            
            // Data message
            var id = NodeObserverIdentifier.FromMessage(gcm);
            OperatorTopologyWithCommunication operatorObserver;
            ConcurrentDictionary<NodeObserverIdentifier, OperatorTopologyWithCommunication> observers;

            if (!_groupMessageObservers.TryGetValue(nsMessage.DestId.ToString(), out observers))
            {
                throw new KeyNotFoundException("Unable to find registered task Observe for destination Task " +
                    nsMessage.DestId + ".");
            }

            if (!observers.TryGetValue(id, out operatorObserver))
            {
                throw new KeyNotFoundException("Unable to find registered Operator Topology for Subscription " +
                    gcm.SubscriptionName + " operator " + gcm.OperatorId);
            }

            operatorObserver.OnNext(nsMessage);
        }

        internal void NextTokenRequest(string taskId, int iteration)
        {
            _ringMessageSource.NextTokenRequest(taskId, iteration);
        }

        internal void NextDataRequest(string taskId)
        {
            _ringMessageSource.NextDataRequest(taskId);
        }

        public void IterationNumber(string taskId, int iteration)
        {
            _ringMessageSource.IterationNumber(taskId, iteration);
        }

        public void JoinTheRing(string taskId, int iteration)
        {
            _ringMessageSource.JoinTheRing(taskId, iteration);
        }

        public void TokenResponse(string taskId, int iteration, bool isTokenReceived)
        {
            _ringMessageSource.TokenResponse(taskId, iteration, isTokenReceived);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
            foreach (var observers in _groupMessageObservers.Values)
            {
                foreach (var observer in observers.Values)
                {
                    observer.OnCompleted();
                }
            }

            _groupMessageObservers.Clear();
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

                foreach (var observers in _groupMessageObservers.Values)
                {
                    foreach (var observer in observers.Values)
                    {
                        observer.OnCompleted();
                    }
                }

                _checkpointService.Dispose();

                _disposed = true;

                Logger.Log(Level.Info, "Group communication layer disposed.");
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

                var leftOvers = foundList.Count == 0 ? identifiers : identifiers.Where(e => !foundList.Contains(e)).ToList();
                var msg = string.Join(",", leftOvers);

                Logger.Log(Level.Error, "Cannot find registered parent/children: {0}.", msg);
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