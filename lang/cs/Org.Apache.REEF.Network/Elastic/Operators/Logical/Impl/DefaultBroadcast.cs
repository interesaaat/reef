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
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Driver.Task;
using System.Collections.Generic;
using System;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Broadcast operator implementation.
    /// </summary>
    class DefaultBroadcast<T> : ElasticOperatorWithDefaultDispatcher, IElasticBroadcast
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultBroadcast<>));

        private volatile bool _stop;

        public DefaultBroadcast(
            int senderId,
            ElasticOperator prev,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null,
                prev,
                topology,
                failureMachine,
                checkpointLevel,
                configurations)
        {
            MasterId = senderId;
            OperatorName = Constants.Broadcast;

            _stop = false;
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder.BindImplementation(GenericType<IElasticTypedOperator<T>>.Class, GenericType<Physical.Impl.DefaultBroadcast<T>>.Class);
            SetMessageType(typeof(Physical.Impl.DefaultBroadcast<T>), ref confBuilder);
        }

        protected override bool ReactOnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            var msgReceived = (TaskMessageType)BitConverter.ToUInt16(message.Message, 0);

            switch (msgReceived)
            {
                case TaskMessageType.JoinTopology:
                    {
                        var operatorId = BitConverter.ToInt32(message.Message, 2);

                        if (operatorId != _id)
                        {
                            return false;
                        }

                        if (!Subscription.Completed && _failureMachine.State.FailureState < (int)DefaultFailureStates.Fail)
                        {
                            var taskId = message.TaskId;
                            LOGGER.Log(Level.Info, "{0} joins the topology", taskId);

                            _topology.AddTask(taskId, ref _failureMachine);
                        }

                        return true;
                    }
                case TaskMessageType.TopologyUpdateRequest:
                    {
                        var operatorId = BitConverter.ToInt32(message.Message, sizeof(ushort));

                        if (operatorId != _id)
                        {
                            return false;
                        }

                        LOGGER.Log(Level.Info, "Received topology update request from {0}", message.TaskId);

                        if (!_stop)
                        {
                            _topology.TopologyUpdateResponse(message.TaskId, ref returnMessages);
                        }
                        else
                        {
                            LOGGER.Log(Level.Info, "Operator {0} is in stopped: Ignoring", OperatorName);
                        }

                        return true;
                    }

                default:
                    return false;
            }
        }

        public override void OnReconfigure(ref IReconfigure reconfigureEvent)
        {
            LOGGER.Log(Level.Info, "Going to reconfigure the broadcast operator");

            if (_stop)
            {
                _stop = false;
            }

            if (reconfigureEvent.FailedTask.IsPresent())
            {
                if (reconfigureEvent.FailedTask.Value.AsError() is OperatorException)
                {
                    if (((OperatorException)reconfigureEvent.FailedTask.Value.AsError()).OperatorId == _id)
                    {
                        var info = Optional<string>.Of(((OperatorException)reconfigureEvent.FailedTask.Value.AsError()).AdditionalInfo);
                        var msg = _topology.Reconfigure(reconfigureEvent.FailedTask.Value.Id, info, reconfigureEvent.Iteration);
                        reconfigureEvent.FailureResponse.AddRange(msg);
                    }
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

        protected override void OnNewIteration(int iteration)
        {
            _topology.OnNewIteration(iteration);

            base.OnNewIteration(iteration);
        }
    }
}
