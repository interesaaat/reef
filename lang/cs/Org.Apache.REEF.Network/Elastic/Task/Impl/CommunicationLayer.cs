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
using Org.Apache.REEF.Network.Elastic.Topology.Task.Impl;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Handles all incoming messages for this Task.
    /// Writable version
    /// </summary>
    internal sealed class CommunicationLayer : IObserver<IRemoteMessage<NsMessage<GroupCommunicationMessage>>>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(CommunicationLayer));

        private readonly int _timeout;
        private readonly int _retryCount;
        private readonly int _sleepTime;
        private readonly StreamingNetworkService<GroupCommunicationMessage> _networkService;
        private readonly IIdentifierFactory _idFactory;

        private readonly ConcurrentDictionary<string, TaskMessageObserver> _taskMessageObservers =
            new ConcurrentDictionary<string, TaskMessageObserver>();

        /// <summary>
        /// A ConcurrentDictionary is used here since there is no ConcurrentSet implementation in C#, and ConcurrentBag
        /// does not allow for us to check for the existence of an item. The byte is simply a placeholder.
        /// </summary>
        private readonly ConcurrentDictionary<string, byte> _registeredNodes = new ConcurrentDictionary<string, byte>();

        /// <summary>
        /// Creates a new GroupCommNetworkObserver.
        /// </summary>
        [Inject]
        private CommunicationLayer(
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.RetryCountWaitingForRegistration))] int retryCount,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.SleepTimeWaitingForRegistration))] int sleepTime,
            StreamingNetworkService<GroupCommunicationMessage> networkService,
            IIdentifierFactory idFactory)
        {
            _timeout = timeout;
            _retryCount = retryCount;
            _sleepTime = sleepTime;
            _networkService = networkService;
            _idFactory = idFactory;
        }

        /// <summary>
        /// Registers a <see cref="TaskMessageObserver"/> for a given <see cref="taskSourceId"/>.
        /// If the <see cref="TaskMessageObserver"/> has already been initialized, it will return
        /// the existing one.
        /// </summary>
        public void RegisterOperatorTopologyForTask(string taskSourceId, OperatorTopology topLayer)
        {
            // Add a TaskMessage observer for each upstream/downstream source.
            var taskObserver = _taskMessageObservers.GetOrAdd(taskSourceId, new TaskMessageObserver(_networkService));
            taskObserver.RegisterNodeObserver(topLayer);
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
            if (message.GetType() == typeof(DataMessage) && ((DataMessage)message).Data == null)
            {
                throw new ArgumentException("Message data cannot be null use a control message instead");
            }

            IIdentifier destId = _idFactory.Create(destination);
            var conn = _networkService.NewConnection(destId);
            conn.Open();
            conn.Write(message);
        }

        /// <summary>
        /// On the first message, we map the <see cref="TaskMessageObserver"/> to the <see cref="IPEndPoint"/>
        /// of the sending Task and register the observer with <see cref="IRemoteManager{T}"/> 
        /// by calling <see cref="TaskMessageObserver#OnNext"/>. On subsequent messages we simply ignore the message
        /// and allow <see cref="ObserverContainer{T}"/> to send the message directly via the <see cref="IPEndPoint"/>.
        /// </summary>
        /// <param name="remoteMessage"></param>
        public void OnNext(IRemoteMessage<NsMessage<GroupCommunicationMessage>> remoteMessage)
        {
            var nsMessage = remoteMessage.Message;
            var gcm = nsMessage.Data.First();
            var gcMessageTaskSource = nsMessage.SourceId.ToString();
            if (!_taskMessageObservers.TryGetValue(gcMessageTaskSource, out TaskMessageObserver observer))
            {
                throw new KeyNotFoundException("Unable to find registered NodeMessageObserver for source Task " +
                    gcMessageTaskSource + ".");
            }

            _registeredNodes.GetOrAdd(gcMessageTaskSource,
                id =>
                {
                    observer.OnNext(remoteMessage);
                    return new byte();
                });
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
                        if (!foundList.Contains(identifier) && _networkService.NamingClient.Lookup(identifier) != null)
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

                var leftOver = string.Join(",", identifiers.Where(e => !foundList.Contains(e)));
                Logger.Log(Level.Error, "Cannot find registered parent/children: {1}.", leftOver);
                throw new RemotingException("Failed to find parent/children nodes");
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        public void Dispose()
        {
        }
    }
}