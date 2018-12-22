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
using System.Collections.Generic;
using Org.Apache.REEF.Common.Tasks;
using System;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Threading;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.NetworkService;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal class ScatterTopology : OneToNTopology
    {
        private readonly int _numElements;

        [Inject]
        private ScatterTopology(
            [Parameter(typeof(OperatorParameters.SubscriptionName))] string subscriptionName,
            [Parameter(typeof(OperatorParameters.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(OperatorParameters.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(OperatorParameters.PiggybackTopologyUpdates))] bool piggyback,
            [Parameter(typeof(OperatorParameters.OperatorId))] int operatorId,
            [Parameter(typeof(OperatorParameters.NumScatterElements))] int numElements,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int disposeTimeout,
            CommunicationService commLayer,
            CentralizedCheckpointService checkpointService) : base(
                taskId,
                Utils.BuildTaskId(subscriptionName, rootId),
                subscriptionName,
                operatorId,
                children,
                piggyback,
                retry,
                timeout,
                disposeTimeout,
                commLayer,
                checkpointService)
        {
            _numElements = numElements;
        }

        public override DataMessage GetDataMessage<T>(int iteration, T[] data)
        {
            if (_piggybackTopologyUpdates)
            {
                return new SplittableDataMessageWithTopology<T>(SubscriptionName, OperatorId, iteration, data);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        protected override void Send(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            int retry = 0;

            if (_sendQueue.TryPeek(out message))
            {
                var dm = message as DataMessage;
                while (!_topologyUpdateReceived.WaitOne(_timeout))
                {
                    if (cancellationSource.IsCancellationRequested)
                    {
                        LOGGER.Log(Level.Warning, "Received cancellation request: stop sending");
                        return;
                    }

                    retry++;

                    if (retry > _retry)
                    {
                        throw new Exception(string.Format(
                            "Iteration {0}: Failed to send message to the next node in the ring after {1} try", dm.Iteration, _retry));
                    }

                    TopologyUpdateRequest();
                }

                _sendQueue.TryDequeue(out message);

                if (TaskId == RootTaskId)
                {
                    _topologyUpdateReceived.Reset();
                }

                var sm = message as SplittableDataMessageWithTopology;
                var children = _children.Values.Where(x => !_nodesToRemove.TryGetValue(x, out byte val));

                foreach (var pair in children.Zip(sm.GetSplits(children.Count()), (lhs, rhs) => Tuple.Create(lhs, rhs)))
                {
                    _commService.Send(pair.Item1, pair.Item2, cancellationSource);
                }
            }
        }
    }
}
