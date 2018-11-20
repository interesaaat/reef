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
using System.Linq;
using Org.Apache.REEF.Wake.Time.Event;
using System.Diagnostics;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Broadcast operator implementation.
    /// </summary>
    class DefaultAggregationRing<T> : ElasticOperatorWithDefaultDispatcher, IElasticAggregationRing
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultAggregationRing<>));

        private volatile bool _stop;

        private double _sum;
        private double _sumSquare;
        private volatile int _count;
        private double _3sigma;
        private Stopwatch _timer;
        private volatile bool _ignoreTimeout;
        private object _lock;

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
            _sum = 0;
            _sumSquare = 0;
            _count = 0;
            _3sigma = 0;
            _ignoreTimeout = false;
            _lock = new object();

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
                case TaskMessageType.JoinTopology:
                    {
                        var operatorId = BitConverter.ToInt16(message.Message, sizeof(ushort));

                        if (operatorId != _id)
                        {
                            return false;
                        }

                        if (!Subscription.IsCompleted && _failureMachine.State.FailureState < (int)DefaultFailureStates.Fail)
                        {
                            LOGGER.Log(Level.Info, "{0} joins the ring", message.TaskId);

                            RingTopology.AddTaskToRing(message.TaskId, _failureMachine);
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

                        LOGGER.Log(Level.Info, "Received token request from {0}", message.TaskId);

                        UpdateTimeoutStatistics();

                        if (!_stop)
                        {
                            RingTopology.TopologyUpdateResponse(message.TaskId, ref returnMessages, Optional<IFailureStateMachine>.Empty());
                        }
                        else
                        {
                            LOGGER.Log(Level.Info, "Operator {0} is in stopped: Ignoring", OperatorName);
                        }
 
                        return true;
                    }
                case TaskMessageType.NextDataRequest:
                    {
                        var iteration = BitConverter.ToInt16(message.Message, sizeof(ushort));
                        LOGGER.Log(Level.Info, "Received next data request from {0} for iteration {1}", message.TaskId, iteration);

                        RingTopology.ResumeRing(ref returnMessages, message.TaskId, iteration);
                        return true;
                    }
                default:
                    return false;
            }
        }

        public override void OnTimeout(Alarm alarm, ref List<IElasticDriverMessage> msgs, ref List<ITimeout> nextTimeouts)
        {
            var isInit = msgs == null;
            var isComputationStarted = _count > 0;
            var id = Subscription.SubscriptionName + "_" + _id;

            if (isInit)
            {
                LOGGER.Log(Level.Warning, "Timeout for Operator {0} in Subscription {1} initialized", _id, Subscription.SubscriptionName);
                ////nextTimeouts.Add(new Timeout(10000, alarm.Handler, Timeout.TimeoutType.Operator, id));

                return;
            }

            if (alarm.GetType() == typeof(OperatorAlarm))
            {
                var opAlarm = alarm as OperatorAlarm;

                if (opAlarm.Id == id)
                {
                    if (isComputationStarted)
                    {
                        if (!_ignoreTimeout)
                        {
                            LOGGER.Log(Level.Info, "Timeout expired, resuming computation");

                            RingTopology.ResumeRing(ref msgs);
                        }

                        lock (_lock)
                        {
                            double avg = _sum / _count;
                            double avgSquared = _sumSquare / _count;
                            double sigma = Math.Sqrt(avgSquared - Math.Pow(avg, 2));
                            double new3Sigma = avg + (3 * sigma);
                            if (_3sigma > new3Sigma)
                            {
                                _3sigma = new3Sigma;
                            }

                            ////nextTimeouts.Add(new Timeout(Math.Max(10000, (long)_3sigma), alarm.Handler, Timeout.TimeoutType.Operator, id));
                        }
                    }
                    else
                    {
                        ////nextTimeouts.Add(new Timeout(10000, alarm.Handler, Timeout.TimeoutType.Operator, id));
                    }

                    _ignoreTimeout = false;
                    return;
                }
            }
                
            if (_next != null)
            {
                _next.OnTimeout(alarm, ref msgs, ref nextTimeouts);
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
                if (reconfigureEvent.FailedTask.IsPresent())
                {
                    if (reconfigureEvent.FailedTask.Value.AsError() is OperatorException)
                    {
                        var info = Optional<string>.Of(((OperatorException)reconfigureEvent.FailedTask.Value.AsError()).AdditionalInfo);
                        var msg = RingTopology.Reconfigure(reconfigureEvent.FailedTask.Value.Id, info, reconfigureEvent.Iteration);
                        reconfigureEvent.FailureResponse.AddRange(msg);
                    }
                    else
                    {
                        var msg = RingTopology.Reconfigure(reconfigureEvent.FailedTask.Value.Id, Optional<string>.Empty(), reconfigureEvent.Iteration);
                        reconfigureEvent.FailureResponse.AddRange(msg);
                    }
                }
            }
            else
            {
                throw new NotImplementedException("No caching is Future work");
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

        private void UpdateTimeoutStatistics()
        {
            _ignoreTimeout = true;
            lock (_lock)
            {
                _count++;

                if (_count == 1)
                {
                    _timer = Stopwatch.StartNew();
                }
                else
                {
                    _timer.Stop();
                    _sum += _timer.ElapsedMilliseconds;
                    _sumSquare += (long)Math.Pow(_timer.ElapsedMilliseconds, 2);
                    _timer.Restart();
                }
            }
        }
    }
}
