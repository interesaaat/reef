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
using System.Collections.Generic;
using System;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Exceptions;
using System.Threading;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.NetworkService;
using System.Collections.Concurrent;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Task;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    /// <summary>
    /// Base class for topologies following a one to N communication pattern.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class OneToNTopology : OperatorTopologyWithDefaultCommunication, ICheckpointingTopology
    {
        protected static readonly Logger LOGGER = Logger.GetLogger(typeof(OneToNTopology));

        private readonly ICheckpointLayer _checkpointLayer;
        protected readonly ConcurrentDictionary<string, byte> _nodesToRemove;

        protected readonly ManualResetEvent _topologyUpdateReceived;
        protected readonly bool _piggybackTopologyUpdates;

        /// <summary>
        /// Construct a one to N topology.
        /// </summary>
        /// <param name="stageName">The stage name the topology is working on</param>
        /// <param name="taskId">The identifier of the task the topology is running on</param>
        /// <param name="rootTaskId">The identifier of the root note in the topology</param>
        /// <param name="operatorId">The identifier of the operator for this topology</param>
        /// <param name="children">The list of nodes this task has to send messages to</param>
        /// <param name="piggyback">Whether to piggyback topology update messages to data message</param>
        /// <param name="retry">How many times the topology will retry to send a message</param>
        /// <param name="timeout">After how long the topology waits for an event</param>
        /// <param name="disposeTimeout">Maximum wait time for topology disposal</param>
        /// <param name="commLayer">Layer responsible for communication</param>
        /// <param name="checkpointLayer">Layer responsible for saving and retrieving checkpoints</param>
        public OneToNTopology(
            string stageName,
            string taskId,
            string rootTaskId,
            int operatorId,
            ISet<int> children,
            bool piggyback,
            int retry,
            int timeout,
            int disposeTimeout,
            DefaultCommunicationLayer commLayer,
            ICheckpointLayer checkpointLayer) : base(stageName, taskId, rootTaskId, operatorId, commLayer, retry, timeout, disposeTimeout)
        {
            _checkpointLayer = checkpointLayer;
            _nodesToRemove = new ConcurrentDictionary<string, byte>();
            _topologyUpdateReceived = new ManualResetEvent(RootTaskId == taskId ? false : true);

            _commLayer.RegisterOperatorTopologyForTask(this);
            _commLayer.RegisterOperatorTopologyForDriver(this);

            _piggybackTopologyUpdates = piggyback;

            foreach (var child in children)
            {
                var childTaskId = Utils.BuildTaskId(StageName, child);

                _children.TryAdd(child, childTaskId);
            }
        }

        /// <summary>
        /// An internal (to the topology) checkpoint. This can be used to implement
        /// ephemeral level checkpoints.
        /// </summary>
        public ICheckpointState InternalCheckpoint { get; private set; }

        /// <summary>
        /// Whether the topology is still sending messages or not.
        /// </summary>
        public bool IsSending
        {
            get { return !_sendQueue.IsEmpty; }
        }

        /// <summary>
        /// Checkpoint the input state for the given iteration.
        /// </summary>
        /// <param name="state">The state to checkpoint</param>
        /// <param name="iteration">The iteration in which the checkpoint is happening</param>

        public void Checkpoint(ICheckpointableState state, int iteration)
        {
            ICheckpointState checkpoint;

            switch (state.Level)
            {
                case CheckpointLevel.None:
                    break;
                case CheckpointLevel.EphemeralMaster:
                    if (TaskId == RootTaskId)
                    {
                        InternalCheckpoint = state.Checkpoint();
                        InternalCheckpoint.Iteration = iteration;
                    }
                    break;
                case CheckpointLevel.EphemeralAll:
                    InternalCheckpoint = state.Checkpoint();
                    InternalCheckpoint.Iteration = iteration;
                    break;
                case CheckpointLevel.PersistentMemoryMaster:
                    if (TaskId == RootTaskId)
                    {
                        checkpoint = state.Checkpoint();
                        checkpoint.OperatorId = OperatorId;
                        checkpoint.Iteration = iteration;
                        checkpoint.StageName = StageName;
                        _checkpointLayer.Checkpoint(checkpoint);
                    }
                    break;
                case CheckpointLevel.PersistentMemoryAll:
                    checkpoint = state.Checkpoint();
                    checkpoint.OperatorId = OperatorId;
                    checkpoint.Iteration = iteration;
                    checkpoint.StageName = StageName;
                    _checkpointLayer.Checkpoint(checkpoint);
                    break;
                default:
                    throw new IllegalStateException($"Checkpoint level {state.Level} not supported.");
            }
        }

        /// <summary>
        /// Retrieve a previously saved checkpoint.
        /// The iteration number specificy which cehckpoint to retrieve, where -1
        /// is used by default to indicate the latest available checkpoint.
        /// </summary>
        /// <param name="checkpoint">The retrieved checkpoint</param>
        /// <param name="iteration">The iteration number for the checkpoint to retrieve.</param>
        /// <returns></returns>
        public bool GetCheckpoint(out ICheckpointState checkpoint, int iteration = -1)
        {
            if (InternalCheckpoint != null && (iteration == -1 || InternalCheckpoint.Iteration == iteration))
            {
                checkpoint = InternalCheckpoint;
                return true;
            }

            return _checkpointLayer.GetCheckpoint(out checkpoint, TaskId, StageName, OperatorId, iteration, false);
        }

        /// <summary>
        /// Waiting logic before disposing topologies. 
        /// </summary>
        public void WaitCompletionBeforeDisposing(CancellationTokenSource cancellationSource)
        {
            if (TaskId == RootTaskId)
            {
                foreach (var node in _children.Values)
                {
                    while (_commLayer.Lookup(node) && !cancellationSource.IsCancellationRequested)
                    {
                        Thread.Sleep(100);
                    }
                }
            }
        }

        public abstract DataMessage GetDataMessage<T>(int iteration, T[] data);

        /// <summary>
        /// Initializes the communication group.
        /// Computation blocks until all required tasks are registered in the group.
        /// </summary>
        /// <param name="cancellationSource">The signal to cancel the operation</param>
        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                _commLayer.WaitForTaskRegistration(_children.Values.ToList(), cancellationSource, _nodesToRemove);
            }
            catch (Exception e)
            {
                throw new IllegalStateException("Failed to find parent/children nodes in operator topology for node: " + TaskId, e);
            }

            _initialized = true;

            Send(cancellationSource);
        }

        /// <summary>
        /// Handler for incoming messages from other topology nodes.
        /// </summary>
        /// <param name="message">The message that need to be devlivered to the operator</param>
        public override void OnNext(NsMessage<ElasticGroupCommunicationMessage> message)
        {
            if (_messageQueue.IsAddingCompleted)
            {
                throw new IllegalStateException("Trying to add messages to a closed non-empty queue.");
            }

            _messageQueue.Add(message.Data);

            if(_piggybackTopologyUpdates)
            {
                var topologyPayload = message.Data as DataMessageWithTopology;
                var updates = topologyPayload.TopologyUpdates;

                UpdateTopology(ref updates);
                topologyPayload.TopologyUpdates = updates;
            }

            if (!_children.IsEmpty)
            {
                _sendQueue.Enqueue(message.Data);
            }

            if (_initialized)
            {
                Send(_cancellationSignal);
            }
        }

        /// <summary>
        /// Handler for messages coming from the driver.
        /// </summary>
        /// <param name="message">Message from the driver</param>
        public override void OnNext(DriverMessagePayload message)
        {
            switch (message.PayloadType)
            {
                case DriverMessagePayloadType.Failure:
                    {
                        var rmsg = message as TopologyMessagePayload;

                        foreach (var updates in rmsg.TopologyUpdates)
                        {
                            foreach (var node in updates.Children)
                            {
                                _nodesToRemove.TryAdd(node, new byte());
                                _commLayer.RemoveConnection(node);
                            }
                        }
                        break;
                    }
                case DriverMessagePayloadType.Update:
                    {
                        if (_sendQueue.Count > 0)
                        {
                            if (_sendQueue.TryPeek(out ElasticGroupCommunicationMessage toSendmsg))
                            {
                                var rmsg = message as TopologyMessagePayload;

                                if (_piggybackTopologyUpdates)
                                {
                                    var toSendmsgWithTop = toSendmsg as DataMessageWithTopology;
                                    var updates = rmsg.TopologyUpdates;

                                    UpdateTopology(ref updates);
                                    toSendmsgWithTop.TopologyUpdates = updates;
                                }

                                foreach (var taskId in _nodesToRemove.Keys)
                                {
                                    var id = Utils.GetTaskNum(taskId);
                                    _nodesToRemove.TryRemove(taskId, out byte val);
                                    _children.TryRemove(id, out string str);
                                }
                            }

                            // Unblock this broadcast round.
                            _topologyUpdateReceived.Set();
                        }
                        else
                        {
                            LOGGER.Log(Level.Warning, "Received a topology update message from driver but sending queue is empty: ignoring.");
                        }
                    }
                    break;
                default:
                    throw new ArgumentException($"Message type {message.PayloadType} not supported by N to one topologies.");
            }
        }

        private void UpdateTopology(ref List<TopologyUpdate> updates)
        {
            TopologyUpdate toRemove = null;
            foreach (var update in updates)
            {
                if (update.Node == TaskId)
                {
                    toRemove = update;
                    foreach (var child in update.Children)
                    { 
                        if (!_nodesToRemove.TryRemove(child, out byte value))
                        {
                            var id = Utils.GetTaskNum(child);
                            _children.TryAdd(id, child);
                        }
                    }
                    break;
                }
            }

            if (toRemove != null)
            {
                updates.Remove(toRemove);
            }
        }
    }
}
