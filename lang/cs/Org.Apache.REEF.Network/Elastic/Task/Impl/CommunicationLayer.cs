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
        private readonly int _retryRegistration;
        private readonly int _retrySending;
        private readonly int _sleepTime;
        private readonly StreamingNetworkService<GroupCommunicationMessage> _networkService;
        private readonly TaskToDriverMessageDispatcher _taskToDriverDispatcher;
        private readonly DriverMessageHandler _driverMessagesHandler;
        private readonly IIdentifierFactory _idFactory;
        private readonly CheckpointService _checkpointService;

        private bool _disposed;
        private IDisposable _disposableObserver;

        private readonly ConcurrentDictionary<NodeObserverIdentifier, OperatorTopologyWithCommunication> _groupMessageObservers =
            new ConcurrentDictionary<NodeObserverIdentifier, OperatorTopologyWithCommunication>();

        private readonly ConcurrentDictionary<NodeObserverIdentifier, DriverAwareOperatorTopology> _driverMessageObservers;

        /// <summary>
        /// Creates a new GroupCommNetworkObserver.
        /// </summary>
        [Inject]
        private CommunicationLayer(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.RetryCountWaitingForRegistration))] int retryRegistration,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SleepTimeWaitingForRegistration))] int sleepTime,
            [Parameter(typeof(ElasticServiceConfigurationOptions.SendRetry))] int retrySending,
            StreamingNetworkService<GroupCommunicationMessage> networkService,
            TaskToDriverMessageDispatcher taskToDriverDispatcher,
            DriverMessageHandler driverMessagesHandler,
            CheckpointService checkpointService,
            IIdentifierFactory idFactory)
        {
            _timeout = timeout;
            _retryRegistration = retryRegistration;
            _sleepTime = sleepTime;
            _retrySending = retrySending;
            _networkService = networkService;
            _taskToDriverDispatcher = taskToDriverDispatcher;
            _driverMessagesHandler = driverMessagesHandler;
            _checkpointService = checkpointService;
            _checkpointService.CommunicationLayer = this;
            _idFactory = idFactory;

            _disposed = false;

            _disposableObserver = _networkService.RemoteManager.RegisterObserver(this);
            _driverMessageObservers = _driverMessagesHandler.DriverMessageObservers;
        }

        /// <summary>
        /// Registers a <see cref="OperatorTopologyWithCommunication"/> for a given <see cref="taskDestinationId"/>.
        /// If the <see cref="OperatorTopologyWithCommunication"/> has already been initialized, it will return
        /// the existing one.
        /// </summary>
        public void RegisterOperatorTopologyForTask(string taskDestinationId, OperatorTopologyWithCommunication operatorObserver)
        {
            var id = NodeObserverIdentifier.FromObserver(operatorObserver);

            if (_groupMessageObservers.ContainsKey(id))
            {
                throw new IllegalStateException("Topology for id " + id + " already added among listeners");
            }

            _groupMessageObservers.TryAdd(id, operatorObserver);
        }

        internal void RegisterOperatorTopologyForDriver(string taskDestinationId, DriverAwareOperatorTopology operatorObserver)
        {
            // Add a TaskMessage observer for each upstream/downstream source.
            var id = NodeObserverIdentifier.FromObserver(operatorObserver);

            if (_driverMessageObservers.ContainsKey(id))
            {
                throw new IllegalStateException("Topology for id " + id + " already added among driver listeners");
            }

            _driverMessageObservers.TryAdd(id, operatorObserver);
        }

        /// <summary>
        /// Send the GroupCommunicationMessage to the Task whose name is
        /// included in the message.
        /// </summary>
        /// <param name="message">The message to send.</param>
        internal void Send(string destination, GroupCommunicationMessage message, CancellationTokenSource cancellationSource)
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
            int retry = 0;

            while (!Send(destId, message))
            {
                if (retry > _retrySending)
                {
                    throw new IllegalStateException("Unable to send message after " + retry + " retry");
                }
                Thread.Sleep(_sleepTime);

                retry++;
            }
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
            var gcm = nsMessage.Data;
            var gcMessageTaskSource = nsMessage.SourceId.ToString();

            if (gcm.GetType() == typeof(CheckpointMessageRequest))
            {
                Logger.Log(Level.Info, "Received checkpoint request from " + gcMessageTaskSource);

                var cpm = gcm as CheckpointMessageRequest;
                ICheckpointState checkpoint;
                if (_checkpointService.GetCheckpoint(out checkpoint, nsMessage.DestId.ToString(), cpm.SubscriptionName, cpm.OperatorId, cpm.Iteration))
                {
                    var returnMessage = checkpoint.ToMessage();
                    var cancellationSource = new CancellationTokenSource();

                    returnMessage.Payload = checkpoint;

                    Send(gcMessageTaskSource, returnMessage, cancellationSource);
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

            if (!_groupMessageObservers.TryGetValue(id, out operatorObserver))
            {
                throw new KeyNotFoundException("Unable to find registered Operator Topology for Subscription " +
                    gcm.SubscriptionName + " operator " + gcm.OperatorId);
            }

            operatorObserver.OnNext(nsMessage);
        }

        internal void TopologyUpdateRequest(string taskId, int operatorId)
        {
            _taskToDriverDispatcher.TopologyUpdateRequest(taskId, operatorId);
        }

        internal void NextDataRequest(string taskId, int iteration)
        {
            _taskToDriverDispatcher.NextDataRequest(taskId, iteration);
        }

        public void IterationNumber(string taskId, int operatorId, int iteration)
        {
            _taskToDriverDispatcher.IterationNumber(taskId, operatorId, iteration);
        }

        public void JoinTopology(string taskId, int operatorId)
        {
            _taskToDriverDispatcher.JoinTopology(taskId, operatorId);
        }

        public void SignalSubscriptionComplete(string taskId)
        {
            _taskToDriverDispatcher.SignalSubscriptionComplete(taskId);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
            foreach (var observer in _groupMessageObservers.Values)
            {
                observer.OnCompleted();
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                OnCompleted();

                _groupMessageObservers.Clear();

                _checkpointService.Dispose();

                _disposableObserver.Dispose();

                _disposed = true;

                Logger.Log(Level.Info, "Group communication layer disposed.");
            }
        }

        /// <summary>
        /// Checks if the identifier is registered with the Name Server.
        /// Throws exception if the operation fails more than the retry count.
        /// </summary>
        /// <param name="identifiers">The identifier to look up</param>
        /// <param name="cancellationSource">The token to cancel the operation</param>
        internal void WaitForTaskRegistration(IList<string> identifiers, CancellationTokenSource cancellationSource, ConcurrentDictionary<string, byte> removed = null)
        {
            if (removed == null)
            {
                removed = new ConcurrentDictionary<string, byte>();
            }

            using (Logger.LogFunction("CommunicationLayer::WaitForTaskRegistration"))
            {
                IList<string> foundList = new List<string>();
                for (var i = 0; i < _retryRegistration; i++)
                {
                    if (cancellationSource != null && cancellationSource.Token.IsCancellationRequested)
                    {
                        Logger.Log(Level.Info, "OperatorTopology.WaitForTaskRegistration is canceled in retryCount {0}.", i);
                        throw new OperationCanceledException("WaitForTaskRegistration is canceled");
                    }

                    Logger.Log(Level.Info, "OperatorTopology.WaitForTaskRegistration, in retryCount {0}.", i);
                    foreach (var identifier in identifiers)
                    {
                        var notFound = !foundList.Contains(identifier);
                        if (notFound && removed.ContainsKey(identifier))
                        {
                            foundList.Add(identifier);
                            Logger.Log(Level.Verbose, "OperatorTopology.WaitForTaskRegistration, dependent id {0} was removed at loop {1}.", identifier, i);
                        }
                        else if (notFound && Lookup(identifier))
                        {
                            foundList.Add(identifier);
                            Logger.Log(Level.Verbose, "OperatorTopology.WaitForTaskRegistration, find a dependent id {0} at loop {1}.", identifier, i);
                        }
                    }

                    if (foundList.Count >= identifiers.Count)
                    {
                        Logger.Log(Level.Info, "OperatorTopology.WaitForTaskRegistration, found all {0} dependent ids at loop {1}.", foundList.Count, i);
                        return;
                    }

                    Thread.Sleep(_sleepTime);
                }

                ICollection<string> leftovers = foundList.Count == 0 ? identifiers : identifiers.Where(e => !foundList.Contains(e)).ToList();
                var msg = string.Join(",", leftovers);

                Logger.Log(Level.Error, "Cannot find registered parent/children: {0}.", msg);
                throw new Exception("Failed to find parent/children nodes");
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

        internal void RemoveConnection(string destination)
        {
            IIdentifier destId = _idFactory.Create(destination);
            _networkService.RemoveConnection(destId);
        }

        private bool Send(IIdentifier destId, GroupCommunicationMessage message)
        {
            var connection = _networkService.NewConnection(destId);
            try
            {
                if (!connection.IsOpen)
                {
                    connection.Open();
                }

                connection.Write(message);
                Console.WriteLine("message sent to {0}", destId);
            }
            catch (Exception e)
            {
                Logger.Log(Level.Warning, "Unable to send message " + e.Message);
                connection.Dispose();
                return false;
            }

            return true;
        }
    }
}