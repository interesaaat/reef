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

using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using System.Globalization;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using System.Collections.Generic;
using Org.Apache.REEF.Driver.Task;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Empty operator implementing the default failure logic. To use only as root.
    /// </summary>
    abstract class ElasticOperatorWithDefaultDispatcher : ElasticOperator, IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticOperatorWithDefaultDispatcher));

        protected ElasticOperatorWithDefaultDispatcher(
            IElasticTaskSetSubscription subscription, 
            ElasticOperator prev, ITopology topology, 
            IFailureStateMachine failureMachine, 
            CheckpointLevel level = CheckpointLevel.None,
            params IConfiguration[] configurations) : 
        base(subscription, prev, topology, failureMachine, level, configurations)
        {
        }

        public override ElasticOperator Broadcast<T>(int senderTaskId, ITopology topology = null, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            _next = new DefaultBroadcast<T>(senderTaskId, this, topology ?? new FlatTopology(senderTaskId), failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
            return _next;
        }

        public override ElasticOperator AggregationRing<T>(int coordinatorTaskId, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            _next = new DefaultAggregationRing<T>(coordinatorTaskId, this, failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
            return _next;
        }

        public override ElasticOperator Reduce(int receiverTaskId, TopologyType topologyType, IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel, params IConfiguration[] configurations)
        {
            _next = new DefaultReduce(receiverTaskId, this, topologyType, failureMachine, checkpointLevel, configurations);
            return _next;
        }

        public override ElasticOperator ConditionalIterate(int coordinatorTaskId, ITopology topology = null, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            _next = new DefaultConditionalIterator(coordinatorTaskId, this, topology ?? new FlatTopology(coordinatorTaskId), failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
            return _next;
        }

        public override ElasticOperator EnumerableIterate(int masterTaskId, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations)
        {
            _next = new DefaultEnumerableIterator(masterTaskId, this, failureMachine ?? _failureMachine.Clone(), checkpointLevel, configurations);
            return _next;
        }

        public override void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents)
        {
            var exception = task.AsError() as OperatorException;

            if (exception.OperatorId <= _id)
            {
                int lostDataPoints = _topology.RemoveTask(task.Id);
                var failureState = _failureMachine.RemoveDataPoints(lostDataPoints);

                switch ((DefaultFailureStates)failureState.FailureState)
                {
                    case DefaultFailureStates.ContinueAndReconfigure:
                        failureEvents.Add(new ReconfigureEvent(task, _id));
                        break;
                    case DefaultFailureStates.ContinueAndReschedule:
                        failureEvents.Add(new RescheduleEvent(task.Id, _id));
                        break;
                    case DefaultFailureStates.StopAndReschedule:
                        failureEvents.Add(new StopEvent(task.Id, _id));
                        break;
                    case DefaultFailureStates.Fail:
                        failureEvents.Add(new FailEvent(task.Id));
                        break;
                    default:
                        break;
                }

                LogOperatorState();
            }

            if (PropagateFailureDownstream() && _next != null)
            {
                _next.OnTaskFailure(task, ref failureEvents);
            }
        }

        public override void EventDispatcher(IFailureEvent @event, ref List<IDriverMessage> failureResponses)
        {
            if (@event.OperatorId == _id)
            {
                switch ((DefaultFailureStateEvents)@event.FailureEvent)
                {
                    case DefaultFailureStateEvents.Reconfigure:
                        failureResponses.AddRange(OnReconfigure(@event as IReconfigure));
                        break;
                    case DefaultFailureStateEvents.Reschedule:
                        failureResponses.AddRange(OnReschedule(@event as IReschedule));
                        break;
                    case DefaultFailureStateEvents.Stop:
                        failureResponses.AddRange(OnStop(@event as IStop));
                        break;
                    default:
                        break;
                }
            }

            if (_next != null && @event.OperatorId > _id)
            {
                _next.EventDispatcher(@event, ref failureResponses);
            }
        }

        public virtual List<IDriverMessage> OnReconfigure(IReconfigure reconfigureEvent)
        {
            return new List<IDriverMessage>();
        }

        public virtual List<IDriverMessage> OnReschedule(IReschedule rescheduleEvent)
        {
            return new List<IDriverMessage>();
        }

        public virtual List<IDriverMessage> OnStop(IStop stopEvent)
        {
            return new List<IDriverMessage>();
        }

        protected override bool PropagateFailureDownstream()
        {
            switch (_failureMachine.State.FailureState)
            {
                case (int)DefaultFailureStates.Continue:
                case (int)DefaultFailureStates.ContinueAndReconfigure:
                case (int)DefaultFailureStates.ContinueAndReschedule:
                    return true;
                default:
                    return false;
            }
        }

        protected override void LogOperatorState()
        {
            string intro = string.Format(CultureInfo.InvariantCulture,
               "State for Operator {0} in Subscription {1}:\n", OperatorName, Subscription.SubscriptionName);
            string topologyState = string.Format(CultureInfo.InvariantCulture, "Topology:\n{0}\n", _topology.LogTopologyState());
            string failureMachineState = "Failure State: " + (DefaultFailureStates)_failureMachine.State.FailureState +
                    "\nFailure(s) Reported: " + _failureMachine.NumOfFailedDataPoints;

            LOGGER.Log(Level.Info, intro + topologyState + failureMachineState);
        }
    }
}
