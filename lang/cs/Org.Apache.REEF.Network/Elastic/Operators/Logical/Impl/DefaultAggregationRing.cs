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
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using System;
using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Broadcast operator implementation.
    /// </summary>
    class DefaultAggregationRing<T> : ElasticOperatorWithDefaultDispatcher, IElasticAggregationRing
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultAggregationRing<>));

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
        }

        private RingTopology RingTopology
        {
            get { return _topology as RingTopology; }
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder.BindImplementation(GenericType<IElasticBasicOperator<T>>.Class, GenericType<Physical.Impl.DefaultAggregationRing<T>>.Class);
            SetMessageType(typeof(Physical.Impl.DefaultAggregationRing<T>), ref confBuilder);
        }

        protected override IEnumerable<DriverMessage> ReactOnTaskMessage(ITaskMessage message)
        {
            var msgReceived = (RingTaskMessageType)BitConverter.ToUInt16(message.Message, 0);

            switch (msgReceived)
            {
                case RingTaskMessageType.JoinTheRing:

                    RingTopology.AddTaskIdToRing(message.TaskId);

                    return RingTopology.GetNextTasksInRing();
                case RingTaskMessageType.TokenReceived:
                    var iteration = BitConverter.ToInt32(message.Message, 2);
                    RingTopology.UpdateTokenPosition(message.TaskId, iteration);
                    return new List<DriverMessage>();
                default:
                    return new List<DriverMessage>();
            }
        }

        protected override bool PropagateFailureDownstream()
        {
            switch (_failureMachine.State.FailureState)
            {
                case (int)DefaultFailureStates.Continue:
                case (int)DefaultFailureStates.ContinueAndReconfigure:
                case (int)DefaultFailureStates.ContinueAndReschedule:
                    return true;
                case (int)DefaultFailureStates.StopAndReschedule:
                    return false;
                default:
                    return false;
            }
        }

        public override IList<DriverMessage> OnReconfigure(IReconfigure reconfigureEvent)
        {
            LOGGER.Log(Level.Info, "Going to reconfigure the ring");

            if (_checkpointLevel > CheckpointLevel.None)
            {
                if (reconfigureEvent.FailedTask.AsError() is OperatorException)
                {
                    var exception = reconfigureEvent.FailedTask.AsError() as OperatorException;
                    if (exception.OperatorId == _id)
                    {
                        return RingTopology.Reconfigure(reconfigureEvent.FailedTask.Id, exception.AdditionalInfo);
                    }
                    else
                    {
                        throw new NotImplementedException("Future work");
                    }
                }
            }
            throw new NotImplementedException("Future work");
        }
    }
}
