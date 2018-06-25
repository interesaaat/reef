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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Driver.Task;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Comm;
using System;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Reduce operator implementation.
    /// </summary>
    internal abstract class DefaultNToOne<T> : ElasticOperatorWithDefaultDispatcher
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultNToOne<>));

        private volatile bool _stop;

        public DefaultNToOne(
            int receiverId,
            ElasticOperator prev,
            TopologyType topologyType,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null, 
                prev, 
                topologyType == TopologyType.Flat ? (ITopology)new FlatTopology(receiverId) : (ITopology)new TreeTopology(receiverId), 
                failureMachine,
                checkpointLevel,
                configurations)
        {
            MasterId = receiverId;
            WithinIteration = prev.WithinIteration;

            _stop = false;
        }

        protected override bool ReactOnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            var msgReceived = (TaskMessageType)BitConverter.ToUInt16(message.Message, 0);

            switch (msgReceived)
            {
                case TaskMessageType.JoinTopology:
                    {
                        var operatorId = BitConverter.ToInt16(message.Message, sizeof(ushort));

                        if (operatorId != _id)
                        {
                            return false;
                        }

                        if (!Subscription.Completed && _failureMachine.State.FailureState < (int)DefaultFailureStates.Fail)
                        {
                            var taskId = message.TaskId;
                            LOGGER.Log(Level.Info, "{0} joins the topology for operator {1}", taskId, _id);

                            _topology.AddTask(taskId, _failureMachine);
                        }

                        return true;
                    }
                case TaskMessageType.TopologyUpdateRequest:
                    {
                        var operatorId = BitConverter.ToInt16(message.Message, sizeof(ushort));

                        if (operatorId != _id)
                        {
                            return false;
                        }

                        LOGGER.Log(Level.Info, "Received topology update request for reduce {0} from {1}", _id, message.TaskId);

                        if (!_stop)
                        {
                            _topology.TopologyUpdateResponse(message.TaskId, ref returnMessages, Optional<IFailureStateMachine>.Of(_failureMachine));
                        }
                        else
                        {
                            LOGGER.Log(Level.Info, "Operator {0} is in stopped: Ignoring", OperatorName);
                        }

                        return true;
                    }

                case TaskMessageType.CompleteSubscription:
                    {
                        Subscription.Completed = true;

                        return true;
                    }

                default:
                    return false;
            }
        }

        public override void OnReconfigure(ref IReconfigure reconfigureEvent)
        {
            LOGGER.Log(Level.Info, "Going to reconfigure the reduce operator");

            if (_stop)
            {
                _stop = false;
            }

            if (reconfigureEvent.FailedTask.IsPresent())
            {
                if (reconfigureEvent.FailedTask.Value.AsError() is OperatorException)
                {
                    var info = Optional<string>.Of(((OperatorException)reconfigureEvent.FailedTask.Value.AsError()).AdditionalInfo);
                    var msg = _topology.Reconfigure(reconfigureEvent.FailedTask.Value.Id, info, reconfigureEvent.Iteration);
                    reconfigureEvent.FailureResponse.AddRange(msg);
                }
                else
                {
                    var msg = _topology.Reconfigure(reconfigureEvent.FailedTask.Value.Id, Optional<string>.Empty(), reconfigureEvent.Iteration);
                    reconfigureEvent.FailureResponse.AddRange(msg);
                }
            }
        }

        public override void OnReschedule(ref IReschedule rescheduleEvent)
        {
            var reconfigureEvent = rescheduleEvent as IReconfigure;

            OnReconfigure(ref reconfigureEvent);
        }

        public override void OnStop(ref IStop stopEvent)
        {
            if (!_stop)
            {
                _stop = true;
            }
        }
    }
}
