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
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Globalization;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using System.Text;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using System.Diagnostics;
using System.Collections.Concurrent;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    /// <summary>
    /// Ring topology for aggregation ring operator.
    /// At configuration time the topology initialize as full connected.
    /// At run-time a ring topology is generated dynamically as nodes join the ring. 
    /// </summary>
    public class RingTopology : ITopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(RingTopology));

        private HashSet<string> _currentWaitingList;
        private HashSet<string> _nextWaitingList;
        private HashSet<string> _tasksInRing;

        // Now I only use current and prev. Eventually one could play with this plus checkpointing,
        // for instance if we checkpoint for up to 5 iterations, we can have an array of prev
        private RingNode _ringHead;
        private StringBuilder _ringPrint;

        private string _rootTaskId;
        private string _taskSubscription;
        private volatile int _iteration;

        private int _rootId;
        private bool _finalized;

        private readonly Dictionary<int, DataNode> _nodes;
        private readonly SortedDictionary<int, Dictionary<string, RingNode>> _ringNodes;

        private HashSet<string> _failedNodesWaiting;
        private HashSet<string> _failedNodes;
        private volatile int _availableDataPoints;

        private readonly object _lock;

        private readonly Stopwatch _timer;
        private long _avg;

        public RingTopology(int rootId)
        {
            _rootId = rootId;
            _finalized = false;
            _availableDataPoints = 0;
            OperatorId = -1;
            SubscriptionName = string.Empty;

            _nodes = new Dictionary<int, DataNode>();
            _ringNodes = new SortedDictionary<int, Dictionary<string, RingNode>>();
            _ringNodes.Add(1, new Dictionary<string, RingNode>());

            _currentWaitingList = new HashSet<string>();
            _nextWaitingList = new HashSet<string>();
            _failedNodesWaiting = new HashSet<string>();
            _failedNodes = new HashSet<string>();
            _ringPrint = new StringBuilder();
            _rootTaskId = string.Empty;
            _taskSubscription = string.Empty;
            _iteration = 1;
            GlobalEvents = new ConcurrentQueue<IElasticDriverMessage>();

            _timer = Stopwatch.StartNew();
            _avg = 0;
            _lock = new object();
        }

        public int OperatorId { get; set; }

        public string SubscriptionName { get; set; }

        public ConcurrentQueue<IElasticDriverMessage> GlobalEvents { get; private set; }

        public int AddTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            var id = Utils.GetTaskNum(taskId);

            lock (_lock)
            {
                if (_nodes.ContainsKey(id))
                {
                    // If the node is not reachable it is recovering.
                    // Don't add it yet to the ring otherwise we slow down closing
                    if (_finalized && _nodes[id].FailState != DataNodeState.Reachable)
                    {
                        if (!_failedNodes.Contains(taskId))
                        {
                            throw new IllegalStateException("Resuming task never failed: Aborting");
                        }

                        _failedNodes.Remove(taskId);
                        _failedNodesWaiting.Add(taskId);
                        _nodes[id].FailState = DataNodeState.Unreachable;

                        return 0;
                    }

                    throw new ArgumentException("Task has already been added to the topology");
                }

                DataNode node = new DataNode(id, false);
                _nodes[id] = node;
                _availableDataPoints++;
            }

            // This is required later in order to build the topology
            if (_taskSubscription == string.Empty)
            {
                _taskSubscription = Utils.GetTaskSubscriptions(taskId);
            }

            return 1;
        }

        public int RemoveTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            var id = Utils.GetTaskNum(taskId);

            if (!_nodes.ContainsKey(id))
            {
                throw new IllegalStateException(string.Format("Task {0} is not part of this topology: ignoring", taskId));
            }

            DataNode node = _nodes[id];

            lock (_lock)
            {
                var state = node.FailState;

                _failedNodesWaiting.Remove(taskId);
                _failedNodes.Add(taskId);
                node.FailState = DataNodeState.Lost;

                if (state != DataNodeState.Reachable)
                {
                    LOGGER.Log(Level.Info, "Removing an already lost node");
                    return 0;
                }

                _availableDataPoints--;
                _tasksInRing.Remove(taskId);
                _currentWaitingList.Remove(taskId);
                _nextWaitingList.Remove(taskId);
                _ringPrint.Replace(taskId + " ", "X ");

                Console.WriteLine("In ring: " + string.Join(", ", _tasksInRing));
                Console.WriteLine("Waiting: " + string.Join(", ", _currentWaitingList));
                Console.WriteLine("Next: " + string.Join(", ", _nextWaitingList));
                Console.WriteLine("Failed Waiting: " + string.Join(", ", _failedNodesWaiting));
                Console.WriteLine("Failed: " + string.Join(", ", _failedNodes));
            }

            return 1;
        }

        public ITopology Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Topology cannot be built more than once");
            }

            if (!_nodes.ContainsKey(_rootId))
            {
                throw new IllegalStateException("Topology cannot be built because the root node is missing");
            }

            if (OperatorId <= 0)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any operator");
            }

            if (SubscriptionName == string.Empty)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any subscription");
            }

            _rootTaskId = Utils.BuildTaskId(_taskSubscription, _rootId);
            _tasksInRing = new HashSet<string> { { _rootTaskId } };
            _ringHead = new RingNode(_rootTaskId, _iteration);
            _ringPrint.Append(_rootTaskId + " ");

            _finalized = true;

            return this;
        }

        public string LogTopologyState()
        {
            lock (_lock)
            {
                return _ringPrint.ToString();
            }
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            foreach (var tId in _nodes.Values)
            {
                if (tId.TaskId != taskId)
                {
                    confBuilder.BindSetEntry<GroupCommunicationConfigurationOptions.TopologyChildTaskIds, int>(
                        GenericType<GroupCommunicationConfigurationOptions.TopologyChildTaskIds>.Class,
                        tId.TaskId.ToString(CultureInfo.InvariantCulture));
                }
            }
            confBuilder.BindNamedParameter<GroupCommunicationConfigurationOptions.TopologyRootTaskId, int>(
                GenericType<GroupCommunicationConfigurationOptions.TopologyRootTaskId>.Class,
                _rootId.ToString(CultureInfo.InvariantCulture));
        }

        internal int AddTaskIdToRing(string taskId, int iteration, ref List<IElasticDriverMessage> messages)
        {
            var addedReachableNodes = 0;

            lock (_lock)
            {
                if (_failedNodes.Contains(taskId))
                {
                    LOGGER.Log(Level.Warning, "Trying to add to the ring a failed task: ignoring");
                    return 0;
                }

                if (_failedNodesWaiting.Contains(taskId))
                {
                    var id = Utils.GetTaskNum(taskId);

                    _availableDataPoints++;
                    _failedNodesWaiting.Remove(taskId);
                    _nodes[id].FailState = DataNodeState.Reachable;
                    addedReachableNodes++;
                }

                if (iteration == -1)
                {
                    int iter = _iteration;
                    while (_ringNodes.TryGetValue(iter, out Dictionary<string, RingNode> dict))
                    {
                        if (dict.TryGetValue(taskId, out RingNode node))
                        {
                            if (node.Next != null)
                            {
                                var data = _ringHead.Type == DriverMessageType.Ring ? (DriverMessagePayload)new RingMessagePayload(node.Next.TaskId, SubscriptionName, OperatorId, _iteration) : (DriverMessagePayload)new FailureMessagePayload(node.Next.TaskId, _iteration, SubscriptionName, OperatorId);
                                var returnMessage = new ElasticDriverMessageImpl(node.TaskId, data);

                                messages.Add(returnMessage);
                            }

                            return 0;
                        }
                        iter--;
                    }

                    return 0;
                }

                if (_currentWaitingList.Contains(taskId) || _tasksInRing.Contains(taskId))
                {
                    _nextWaitingList.Add(taskId);
                }
                else
                {
                    _currentWaitingList.Add(taskId);
                }
            }

            return addedReachableNodes;
        }

        internal void GetNextTasksInRing(ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                while (_currentWaitingList.Count > 0)
                {
                    var nextTask = _currentWaitingList.First();
                    var dest = _ringHead.TaskId;
                    var data = _ringHead.Type == DriverMessageType.Ring ? (DriverMessagePayload)new RingMessagePayload(nextTask, SubscriptionName, OperatorId, _iteration) : (DriverMessagePayload)new FailureMessagePayload(nextTask, _iteration, SubscriptionName, OperatorId);
                    var returnMessage = new ElasticDriverMessageImpl(dest, data);

                    messages.Add(returnMessage);
                    Console.WriteLine("Task {0} sends to {1} in iteration {2} in next", dest, nextTask, _iteration);
                    _ringHead.Next = new RingNode(nextTask, _iteration, _ringHead);
                    _ringHead = _ringHead.Next;
                    _tasksInRing.Add(nextTask);
                    _currentWaitingList.Remove(nextTask);
                    _ringPrint.Append("-> " + nextTask + " ");
                    _ringNodes[_iteration].Add(nextTask, _ringHead);

                    CloseRing(ref messages);
                }
            }

            // Continuously build the ring until there is some node waiting
            if (_currentWaitingList.Count > 0)
            {
                GetNextTasksInRing(ref messages);
            }
        }

        internal void CloseRing(ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                if (_availableDataPoints <= _tasksInRing.Count)
                {
                    var dest = _ringHead.TaskId;
                    var data = _ringHead.Type == DriverMessageType.Ring ? (DriverMessagePayload)new RingMessagePayload(_rootTaskId, SubscriptionName, OperatorId, _iteration) : (DriverMessagePayload)new FailureMessagePayload(_rootTaskId, _iteration, SubscriptionName, OperatorId);
                    var returnMessage = new ElasticDriverMessageImpl(dest, data);
                    Console.WriteLine("Task {0} sends to {1} in iteration {2} in close", dest, _rootTaskId, _iteration);
                    messages.Add(returnMessage);
                    _timer.Stop();
                    LOGGER.Log(Level.Info, "Ring in Iteration {0} is closed in {1}ms with {2} nodes:\n {3} -> {4}", _iteration, _timer.ElapsedMilliseconds, _tasksInRing.Count, LogTopologyState(), _rootTaskId);

                    _ringHead.Next = new RingNode(_rootTaskId, _iteration, _ringHead);
                    _ringHead = _ringHead.Next;
                    _currentWaitingList = _nextWaitingList;
                    _nextWaitingList = new HashSet<string>();
                    _tasksInRing = new HashSet<string> { { _rootTaskId } };
                    _ringNodes[_iteration].Add(_rootTaskId, _ringHead);
                    _avg += _timer.ElapsedMilliseconds;
                    ResumeRing(_iteration + 1, (int)(_avg / _iteration));

                    _iteration++;
                    _ringPrint = new StringBuilder(_rootTaskId + " ");
                    _ringNodes.Add(_iteration, new Dictionary<string, RingNode>());
                    CleanPreviousRings();

                    _timer.Restart();
                }
            }
        }

        internal void RetrieveTokenFromRing(string taskId, int iteration, ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                RingNode node;
                if (!_ringNodes[iteration].TryGetValue(taskId, out node))
                {
                    var msg = "Node not found: ";
                    if (_currentWaitingList.Count > 0 || _failedNodesWaiting.Count > 0)
                    {
                        LOGGER.Log(Level.Warning, msg + "going to flush nodes in queue");

                        GetNextTasksInRing(ref messages);
                    }
                    else
                    {
                        LOGGER.Log(Level.Warning, msg + " waiting");
                    }
                }

                if (node.Next != null)
                {
                    var dest = taskId;
                    var data = node.Type == DriverMessageType.Ring ? (DriverMessagePayload)new RingMessagePayload(node.Next.TaskId, SubscriptionName, OperatorId, node.Iteration) : (DriverMessagePayload)new FailureMessagePayload(node.Next.TaskId, node.Iteration, SubscriptionName, OperatorId);
                    messages.Add(new ElasticDriverMessageImpl(dest, data));
                    Console.WriteLine("Task {0} sends to {1} in iteration {2} in retrieve", dest, node.Next.TaskId, node.Iteration);
                }

                LOGGER.Log(Level.Info, "Next token is {0}", node.Next.TaskId);    
            }
        }

        internal void RetrieveMissedDataFromRing(string taskId, int iteration, ref List<IElasticDriverMessage> returnMessages)
        {
            lock (_lock)
            {
                if (taskId == _rootTaskId)
                {
                    iteration -= 2;
                }
                RingNode node;
                if (_ringNodes[iteration].TryGetValue(taskId, out node) && node.Prev != null)
                {
                    var dest = node.Prev.TaskId;
                    var data = new ResumeMessagePayload(node.TaskId, node.Iteration, SubscriptionName, OperatorId);
                    returnMessages.Add(new ElasticDriverMessageImpl(dest, data));
                    Console.WriteLine("Task {0} sends to {1} in iteration {2} in resume data", dest, node.TaskId, node.Iteration);
                }
                else
                {
                    LOGGER.Log(Level.Warning, "Impossible to retrieve missing data for Task {0} in iteration {1}", taskId, iteration);
                }
            }
        }

        internal void ResumeRingFromCheckpoint(string taskId, int iteration, ref List<IElasticDriverMessage> messages)
        {
            lock (_lock)
            {
                RingNode node;
                if (_ringNodes[iteration].TryGetValue(taskId, out node) && node.Prev != null)
                {
                    var data = new FailureMessagePayload(node.TaskId, node.Iteration, SubscriptionName, OperatorId);
                    var returnMessage = new ElasticDriverMessageImpl(node.Prev.TaskId, data);

                    node.Type = DriverMessageType.Ring;
                    messages.Add(returnMessage);
                    Console.WriteLine("Task {0} sends to {1} in iteration {2} in resume", node.Prev.TaskId, node.TaskId, node.Iteration);

                    LOGGER.Log(Level.Info, "Resuming ring from node {0}", node.Prev.TaskId);
                }
                else
                {
                    LOGGER.Log(Level.Warning, "Impossible to resume from checkpoint for Task {0} in iteration {1}", taskId, iteration);
                }
            }
        }

        public IList<IElasticDriverMessage> Reconfigure(string taskId, string info)
        {
            if (taskId == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet");
            }

            PrintRing();

            List<IElasticDriverMessage> messages = new List<IElasticDriverMessage>();
            var failureInfos = info.Split(':');
            int position = int.Parse(failureInfos[0]);
            int iteration = int.Parse(failureInfos[1]);

            Console.WriteLine(taskId + " failure " + info);

            lock (_lock)
            {
                Console.WriteLine("Failure in {0} at iteration {1}", taskId, iteration);

                if (_ringNodes.TryGetValue(iteration, out Dictionary<string, RingNode> nodes))
                {
                    nodes.TryGetValue(taskId, out RingNode node);

                    switch (position)
                    {
                        // We are before receive, we should be ok
                        case (int)PositionTracker.Nil:
                            LOGGER.Log(Level.Info, "Node failed before any communication: no need to reconfigure");

                            break;

                        // The failure is on the node with token
                        case (int)PositionTracker.InReceive:
                        case (int)PositionTracker.AfterReceiveBeforeSend:
                            if (node == null)
                            {
                                if (!_ringNodes[_iteration].TryGetValue(taskId, out node))
                                {
                                    throw new IllegalStateException(string.Format("Failure in {0} in iteration {1} not recognized: current ring is in {2}", taskId, iteration, _iteration));
                                }
                            }

                            var next = node.Next;
                            var prev = node.Prev;

                            // We are at the end of the ring
                            if (next == null)
                            {
                                _ringHead = prev;
                                _ringHead.Type = DriverMessageType.Failure;
                            }
                            else
                            {
                                var data = new FailureMessagePayload(next.TaskId, prev.Iteration, SubscriptionName, OperatorId);
                                var returnMessage = new ElasticDriverMessageImpl(prev.TaskId, data);
                                messages.Add(returnMessage);
                                Console.WriteLine("Task {0} sends to {1} in iteration {2} in receive", prev.TaskId, next.TaskId, prev.Iteration);
                            }
                            LOGGER.Log(Level.Info, "Sending reconfiguration message: restarting from node {0}", node.TaskId);
                            break;

                        // The failure is on the node with token while sending
                        case (int)PositionTracker.InSend:
                        // We are after send but before a new iteration starts 
                        // Data may or may not have reached the next node
                        case (int)PositionTracker.AfterSendBeforeReceive:
                            if (node == null)
                            {
                                if (!_ringNodes[_iteration].TryGetValue(taskId, out node))
                                {
                                    throw new IllegalStateException(string.Format("Failure in {0} in iteration {1} not recognized: current ring is in {2}", taskId, iteration, _iteration));
                                }
                            }

                            next = node.Next;
                            prev = node.Prev;

                            // We are at the end of the ring
                            if (next == null)
                            {
                                _ringHead = prev;
                                _ringHead.Type = DriverMessageType.Failure;
                            }
                            else
                            {
                                next.Type = DriverMessageType.Request;

                                var data = new TokenReceivedRequest(iteration, SubscriptionName, OperatorId);
                                var returnMessage = new ElasticDriverMessageImpl(next.TaskId, data);
                                messages.Add(returnMessage);

                                LOGGER.Log(Level.Info, "Sending request token message to node {0} for iteration {1}", next.TaskId, iteration);
                            }
                            break;
                        default:
                            break;
                    }
                }

                foreach (var dict in _ringNodes.Values)
                {
                    if (dict.TryGetValue(taskId, out RingNode node))
                    {
                        if (node.Next != null)
                        {
                            node.Next.Prev = node.Prev;
                        }
                        if (node.Prev != null)
                        {
                            node.Prev.Next = node.Next;
                        }

                        node = null;
                        dict.Remove(taskId);
                    }
                }
            }

            CloseRing(ref messages);

            return messages;
        }

        private void CleanPreviousRings()
        {
            lock (_lock)
            {
                if (_ringNodes.Count > 3)
                {
                    var smallerDict = _ringNodes.First();
                    var keys = smallerDict.Value.Keys.ToArray();

                    for (int i = 0; i < keys.Length; i++)
                    {
                        var task = keys[i];
                        smallerDict.Value[task].Prev = null;
                        smallerDict.Value[task].Next = null;
                        smallerDict.Value[task] = null;
                    }

                    _ringNodes.Remove(smallerDict.Key);
                }
            }
        }

        private async void ResumeRing(int iteration, int delay = 10000)
        {
            await System.Threading.Tasks.Task.Delay(delay);
            if (_iteration == iteration)
            {
                ////Console.WriteLine("Resuming ring");
                ////var messages = new List<IElasticDriverMessage>();
                ////RetrieveMissedDataFromRing(_rootTaskId, iteration, ref messages);

                ////foreach (var x in messages)
                ////{
                ////    GlobalEvents.Enqueue(x);
                ////}
            }
        }

        private void PrintRing()
        {
            lock (_lock)
            { 
                StringBuilder str = new StringBuilder();
                var head = _ringHead;

                while (head != null)
                {
                    str.Append(head.TaskId + " " + head.Iteration + " <- ");
                    head = head.Prev;
                }

                Console.WriteLine(str.ToString()); 
            }
        }
    }

    internal class RingNode
    {
        public RingNode(string taskId, int iteration, RingNode prev = null)
        {
            TaskId = taskId;
            Iteration = iteration;
            Next = null;
            Prev = prev;
            Type = DriverMessageType.Ring;
        }

        public string TaskId { get; private set; }

        public int Iteration { get; set; }

        public RingNode Next { get; set; }

        public RingNode Prev { get; set; }

        public DriverMessageType Type { get; set; }
    }
}
