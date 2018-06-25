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
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Tang.Util;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using System;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using System.Globalization;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities;
using System.Diagnostics;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Iterate operator implementation.
    /// </summary>
    class DefaultEnumerableIterator : ElasticOperatorWithDefaultDispatcher, IElasticIterator
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultEnumerableIterator));

        private volatile int _iteration;
        private readonly int _numIterations;

        private readonly Stopwatch _timer;
        private long _totTime;

        private bool _isLast;

        public DefaultEnumerableIterator(
            int masterTaskId,
            ElasticOperator prev,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null,
                prev,
                new RootTopology(masterTaskId),
                failureMachine,
                checkpointLevel,
                configurations)
        {
            MasterId = masterTaskId;
            OperatorName = Constants.Iterate;
            WithinIteration = true;
            _isLast = false;
            _iteration = 0;

            foreach (var conf in _configurations)
            {
                foreach (INamedParameterNode opt in conf.GetNamedParameters())
                {
                    if (opt.GetName() == typeof(NumIterations).FullName)
                    {
                        _numIterations = int.Parse(conf.GetNamedParameter(opt));
                    }
                }
            }
            _timer = new Stopwatch();
            _totTime = 0;
        }

        /// <summary>
        /// Finalizes the Operator.
        /// </summary>
        /// <returns>The same finalized Operator</returns>
        public override ElasticOperator Build()
        {
            if (_operatorFinalized == true)
            {
                throw new IllegalStateException("Operator cannot be built more than once");
            }

            if (_prev != null)
            {
                _prev.Build();
            }

            Subscription.IsIterative = true;

            // Pipelines can have more then 1 iterator: make the last one aware that it has to 
            // complete the subscription when done
            _isLast = _next.CheckIfLastIterator();

            _operatorFinalized = true;

            return this;
        }

        public override bool CheckIfLastIterator()
        {
            return false;
        }

        public override bool AddTask(string taskId)
        {
            if (_operatorFinalized == false)
            {
                throw new IllegalStateException("Operator needs to be built before adding tasks");
            }

            _topology.AddTask(taskId, _failureMachine);

            if (_next != null)
            {
                // A task is new if it got added by at least one operator
                return _next.AddTask(taskId);
            }
            else
            {
                // If iterate is the only operator, by default each node is new
                return true;
            }
        }

        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
            confBuilder
                .BindImplementation(GenericType<IElasticTypedOperator<int>>.Class, GenericType<Physical.Impl.DefaultEnumerableIterator>.Class);
            SetMessageType(typeof(IElasticTypedOperator<int>), ref confBuilder);
        }

        protected override bool ReactOnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            var msgReceived = (TaskMessageType)BitConverter.ToUInt16(message.Message, 0);

            switch (msgReceived)
            {
                case TaskMessageType.IterationNumber:
                    var operatorId = BitConverter.ToInt16(message.Message, sizeof(ushort));

                    if (operatorId != _id)
                    {
                        return false;
                    }

                    var newIteration = Math.Max(_iteration, BitConverter.ToUInt16(message.Message, sizeof(ushort) + sizeof(ushort)));

                    if (_iteration < newIteration)
                    {
                        OnNewIteration(newIteration);
                    }

                    return true;
                default:
                    return false;
            }
        }

        public override void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents)
        {
            var failedOperatorId = _id;

            if (task.AsError() is OperatorException)
            {
                var opException = task.AsError() as OperatorException;
                failedOperatorId = opException.OperatorId;
            }

            if (failedOperatorId >= _id)
            {
                int lostDataPoints = _topology.RemoveTask(task.Id);
                var failureState = _failureMachine.RemoveDataPoints(lostDataPoints);
            }

            if (PropagateFailureDownstream() && _next != null)
            {
                _next.OnTaskFailure(task, ref failureEvents);
            }
        }

        public override void OnReconfigure(ref IReconfigure reconfigureEvent)
        {
            if (reconfigureEvent.FailedTask.IsPresent() && !(reconfigureEvent.FailedTask.Value.AsError() is OperatorException))
            {
                reconfigureEvent.Iteration = Optional<int>.Of(_iteration);
            }
        }

        public override void OnReschedule(ref IReschedule rescheduleEvent)
        {
            LOGGER.Log(Level.Info, "Going to reschedule task " + rescheduleEvent.TaskId);

            if (_iteration < _numIterations)
            {
                var reconfigureEvent = rescheduleEvent as IReconfigure;
                var checkpointConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<StartIteration, int>(
                    GenericType<StartIteration>.Class,
                    0.ToString(CultureInfo.InvariantCulture))
                .Build();

                if (!rescheduleEvent.RescheduleTaskConfigurations.TryGetValue(Subscription.SubscriptionName, out IList<IConfiguration> confs))
                {
                    confs = new List<IConfiguration>();
                    rescheduleEvent.RescheduleTaskConfigurations.Add(Subscription.SubscriptionName, confs);
                }
                confs.Add(checkpointConf);

                OnReconfigure(ref reconfigureEvent);
            }
        }

        protected override bool PropagateFailureDownstream()
        {
            // We don't expect iterator operators to fail
            return true;
        }

        protected override string LogInternalStatistics()
        {
            var actualIteration = _iteration > 2 ? _iteration - 1 : 1;
            return string.Format("\nNumber of Iterations {0}\nTotal computation time {1}s\nAverage iteration time {2}ms", Math.Min(actualIteration, _numIterations), (float)_totTime / 1000.0, _totTime / actualIteration);
        }

        private new void OnNewIteration(int iteration)
        {
            _timer.Stop();
            _totTime += _timer.ElapsedMilliseconds;

            _iteration = iteration;

            if (_iteration > _numIterations)
            {
                if (_isLast)
                {
                    Subscription.Completed = true;
                }
            }
            else
            {
                LOGGER.Log(Level.Info, "Starting iteration " + _iteration);
            }

            if (_iteration > 1)
            {
                LOGGER.Log(Level.Info, string.Format("Iteration {0} is closed in {1}ms", _iteration - 1, _timer.ElapsedMilliseconds));

                base.OnNewIteration(iteration);
            }

            _timer.Restart();
        }
    }
}
