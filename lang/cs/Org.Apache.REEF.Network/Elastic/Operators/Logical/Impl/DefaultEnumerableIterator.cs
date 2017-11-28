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

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Iterate operator implementation.
    /// </summary>
    class DefaultEnumerableIterator : ElasticOperatorWithDefaultDispatcher, IElasticIterator
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultEnumerableIterator));

        private int _iteration;
        private int _numIterations;

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
        }

        internal override void GatherMasterIds(ref HashSet<string> missingMasterTasks)
        {
            if (_operatorFinalized != true)
            {
                throw new IllegalStateException("Operator need to be build before finalizing the subscription");
            }

            if (_next != null)
            {
                _next.GatherMasterIds(ref missingMasterTasks);
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
                    _iteration = Math.Max(_iteration, BitConverter.ToUInt16(message.Message, 2));

                    if (_iteration > _numIterations)
                    {
                        Subscription.Completed = true;
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

        public override void OnResume(ref List<IElasticDriverMessage> msgs, ref string taskId, ref int? iteration)
        {
            iteration = _iteration;
            
            if (_next != null)
            {
                OnResume(ref msgs, ref taskId, ref iteration);
            }
        }

        public override void OnReconfigure(ref IReconfigure reconfigureEvent)
        {
            if (reconfigureEvent.FailedTask.IsPresent())
            {
                if (!(reconfigureEvent.FailedTask.Value.AsError() is OperatorException))
                {
                    reconfigureEvent.Iteration = Optional<int>.Of(_iteration);
                }
            }
        }

        public override void OnReschedule(ref IReschedule rescheduleEvent)
        {
            LOGGER.Log(Level.Info, "Going to reschedule task " + rescheduleEvent.TaskId);

            if (_checkpointLevel == CheckpointLevel.None)
            {
                throw new NotImplementedException("Future work");
            }

            if (_iteration < _numIterations)
            {
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
            }
        }

        protected override bool PropagateFailureDownstream()
        {
            // We don't expect iterator operators to fail
            return true;
        }

        protected override string LogInternalStatistics()
        {
            return "Number of Iterations: " + Math.Min(_iteration, _numIterations);
        }
    }
}
