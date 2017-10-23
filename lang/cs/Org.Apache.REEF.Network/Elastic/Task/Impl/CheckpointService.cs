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

using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Interface for checkpointing some task state
    /// Clients can implement this interface and inject it into context service and task function to save the current task state
    /// </summary>
    internal class CheckpointService : IDisposable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(CheckpointService));

        private readonly ConcurrentDictionary<CheckpointIdentifier, SortedDictionary<int, ICheckpointState>> _checkpoints;
        private readonly ConcurrentDictionary<CheckpointIdentifier, string> _roots;
        private readonly ConcurrentDictionary<CheckpointIdentifier, ManualResetEvent> _checkpointsWaiting;

        private readonly int _limit;

        private CommunicationLayer _communicationLayer;

        [Inject]
        public CheckpointService(
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumCheckpoints))] int num,
            StreamingNetworkService<GroupCommunicationMessage> networkService)
        {
            _limit = num;

            _checkpoints = new ConcurrentDictionary<CheckpointIdentifier, SortedDictionary<int, ICheckpointState>>();
            _roots = new ConcurrentDictionary<CheckpointIdentifier, string>();
            _checkpointsWaiting = new ConcurrentDictionary<CheckpointIdentifier, ManualResetEvent>();
        }

        public CommunicationLayer CommunicationLayer
        {
            set { _communicationLayer = value; }
        }

        public void RegisterOperatorRoot(string subscriptionName, int operatorId, string rootTaskId, bool amIRoot)
        {
            var id = new CheckpointIdentifier(string.Empty, subscriptionName, operatorId);
            if (!_roots.ContainsKey(id) && !amIRoot)
            {
                _roots.TryAdd(id, rootTaskId);
            }
        }

        public ICheckpointState GetCheckpoint(string taskId, string subscriptionName, int operatorId, int iteration = -1)
        {
            SortedDictionary<int, ICheckpointState> checkpoints;
            var id = new CheckpointIdentifier(taskId, subscriptionName, operatorId);
            if (!_checkpoints.TryGetValue(id, out checkpoints))
            {
                Logger.Log(Level.Warning, "Asking for a checkpoint not in the service");

                var id2 = new CheckpointIdentifier(string.Empty, subscriptionName, operatorId);
                string rootTaskId;

                if (!_roots.TryGetValue(id2, out rootTaskId))
                {
                    // I am in root, try to fetch as root
                    if (!_checkpoints.TryGetValue(id, out checkpoints))
                    {
                        throw new IllegalStateException("Trying to recover from a non existing checkpoint");
                    }
                }

                Logger.Log(Level.Info, "Retrieving the checkpoint from {0}", rootTaskId);
                var cpm = new CheckpointMessageRequest(subscriptionName, operatorId, iteration);

                _communicationLayer.Send(rootTaskId, cpm);

                var received = new ManualResetEvent(false);

                _checkpointsWaiting.TryAdd(id, received);

                received.WaitOne();

                if (!_checkpoints.TryGetValue(id, out checkpoints))
                {
                    throw new IllegalStateException("Checkpoint not retrieved");
                }
            }

            iteration = iteration < 0 ? checkpoints.Keys.Last() : iteration;

            return checkpoints[iteration];
        }

        public void Checkpoint(ICheckpointState state)
        {
            SortedDictionary<int, ICheckpointState> checkpoints;
            var id = new CheckpointIdentifier(state.TaskId, state.SubscriptionName, state.OperatorId);
            ManualResetEvent waiting;

            if (!_checkpoints.TryGetValue(id, out checkpoints))
            {
                checkpoints = new SortedDictionary<int, ICheckpointState>();
                _checkpoints.TryAdd(id, checkpoints);
            }

            checkpoints.Add(state.Iteration, state);

            if (_checkpointsWaiting.TryRemove(id, out waiting))
            {
                waiting.Set();
            }

            CheckSize(checkpoints);
        }
        
        public void RemoveCheckpoint(string taskId, string subscriptionName, int operatorId)
        {
            var id = new CheckpointIdentifier(taskId, subscriptionName, operatorId);
            SortedDictionary<int, ICheckpointState> checkpoints;
            _checkpoints.TryRemove(id, out checkpoints);
        }

        public void Dispose()
        {
            foreach (var waiting in _checkpointsWaiting.Values)
            {
                waiting.Set();
                waiting.Close();
            }
        }

        private void CheckSize(SortedDictionary<int, ICheckpointState> checkpoint)
        {
            if (checkpoint.Keys.Count > _limit)
            {
                var first = checkpoint.Keys.First();
                checkpoint.Remove(first);
            }
        }
    }
}
