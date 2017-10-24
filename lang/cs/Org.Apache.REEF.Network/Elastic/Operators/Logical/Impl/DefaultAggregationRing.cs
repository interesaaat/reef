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
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities.Logging;
using System.Collections.Generic;
using System;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Broadcast operator implementation.
    /// </summary>
    class DefaultAggregationRing<T> : ElasticOperatorWithDefaultDispatcher, IElasticAggregationRing
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultAggregationRing<>));
        private volatile bool _stop;

        public DefaultAggregationRing(
            int coordinatorId,
            ElasticOperator prev,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null,
                prev,
                new RingTopology(coordinatorId),
                failureMachine,
                checkpointLevel,
                configurations)
        {
            MasterId = coordinatorId;
            OperatorName = Constants.AggregationRing;

            _stop = false;
        }

        private RingTopology RingTopology
        {
            get { return _topology as RingTopology; }
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder.BindImplementation(GenericType<IElasticTypedOperator<T>>.Class, GenericType<Physical.Impl.DefaultAggregationRing<T>>.Class);
            SetMessageType(typeof(Physical.Impl.DefaultAggregationRing<T>), ref confBuilder);
        }

        protected override bool ReactOnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            var msgReceived = (TaskMessageType)BitConverter.ToUInt16(message.Message, 0);

            switch (msgReceived)
            {
                case TaskMessageType.JoinTheRing:
                    {
                        if (!Subscription.Completed && _failureMachine.State.FailureState < (int)DefaultFailureStates.Fail)
                        {
                            Console.WriteLine("Task {0} is going to join the ring", message.TaskId);
                            var iteration = BitConverter.ToInt32(message.Message, sizeof(ushort));
                            var addedDataPoints = RingTopology.AddTaskIdToRing(message.TaskId, iteration);
                            _failureMachine.AddDataPoints(addedDataPoints);

                            if (!_stop)
                            {
                                RingTopology.GetNextTasksInRing(ref returnMessages);
                            } 
                        }

                        return true;
                    }
                case TaskMessageType.TokenResponse:
                    {
                        if (message.Message[6] == 0)
                        {
                            if (_checkpointLevel > CheckpointLevel.None)
                            {
                                var iteration = BitConverter.ToInt32(message.Message, sizeof(ushort));
                                RingTopology.ResumeRingFromCheckpoint(message.TaskId, iteration, ref returnMessages);
                            }
                            else
                            {
                                throw new NotImplementedException("Future work");
                            }
                        }
                        else
                        {
                            LOGGER.Log(Level.Info, "Node {0} received token: no need to reconfigure", message.TaskId);
                        }

                        return true;
                    }
                case TaskMessageType.NextTokenRequest:
                    {
                        var iteration = BitConverter.ToInt32(message.Message, sizeof(ushort));
                        LOGGER.Log(Level.Info, "Received next token request for iteration {0} from node {1}", iteration, message.TaskId);

                        RingTopology.RetrieveTokenFromRing(message.TaskId, iteration, ref returnMessages);
                        return true;
                    }
                case TaskMessageType.NextDataRequest:
                    {
                        LOGGER.Log(Level.Info, "Received next data request from node {0}", message.TaskId);

                        RingTopology.RetrieveMissedDataFromRing(message.TaskId, ref returnMessages);
                        return true;
                    }
                default:
                    return false;
            }
        }

        public override void OnReconfigure(ref IReconfigure reconfigureEvent)
        {
            LOGGER.Log(Level.Info, "Going to reconfigure the ring");

            if (_stop)
            {
                _stop = false;
            }

            if (_checkpointLevel > CheckpointLevel.None)
            {
                if (reconfigureEvent.FailedTask.AsError() is OperatorException)
                {
                    var exception = reconfigureEvent.FailedTask.AsError() as OperatorException;
                    if (exception.OperatorId == _id)
                    {
                        reconfigureEvent.FailureResponse.AddRange(RingTopology.Reconfigure(reconfigureEvent.FailedTask.Id, exception.AdditionalInfo));
                    }
                    else
                    {
                        throw new NotImplementedException("Different operator id is Future work");
                    }
                }
            }
            else
            {
                throw new NotImplementedException("No catching is Future work");
            }
        }

        public override void OnReschedule(ref IReschedule rescheduleEvent)
        {
            LOGGER.Log(Level.Info, "Going to reconfigure the ring");

            if (_stop)
            {
                _stop = false;
            }

            if (_checkpointLevel > CheckpointLevel.None)
            {
                if (rescheduleEvent.FailedTask.IsPresent() && rescheduleEvent.FailedTask.Value.AsError() is OperatorException)
                {
                    var exception = rescheduleEvent.FailedTask.Value.AsError() as OperatorException;
                    if (exception.OperatorId == _id)
                    {
                        rescheduleEvent.FailureResponse.AddRange(RingTopology.Reconfigure(rescheduleEvent.TaskId, exception.AdditionalInfo));
                    }
                }
            }
            else
            {
                throw new NotImplementedException("No caching is Future work");
            }
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
