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

using System;
using System.Linq;
using System.Globalization;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    /// <summary>
    /// Example implementation of a broadcast and reduce pipeline using the elastic group communication service.
    /// </summary>
    public class ElasticIterateAllReduceDriver :
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<IDriverStarted>,
        IObserver<IRunningTask>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedTask>,
        IObserver<ITaskMessage>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticIterateBroadcastReduceDriver));

        private readonly int _numEvaluators;
        private readonly int _numIterations;

        private readonly IConfiguration _tcpPortProviderConfig;
        private readonly IConfiguration _codecConfig;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private readonly IElasticTaskSetService _service;
        private readonly IElasticTaskSetSubscription _subscription;
        private readonly ITaskSetManager _taskManager;

        [Inject]
        private ElasticIterateAllReduceDriver(
            [Parameter(typeof(NumIterations))] int numIterations,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(ElasticServiceConfigurationOptions.StartingPort))] int startingPort,
            [Parameter(typeof(ElasticServiceConfigurationOptions.PortRange))] int portRange,
            IElasticTaskSetService service,
            IEvaluatorRequestor evaluatorRequestor)
        {
            _numIterations = numIterations;
            _numEvaluators = numEvaluators;
            _service = service;
            _evaluatorRequestor = evaluatorRequestor;

            _tcpPortProviderConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class,
                    startingPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class,
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            _codecConfig = StreamingCodecConfiguration<int>.Conf
                .Set(StreamingCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                .Build();

            IConfiguration reduceFunctionConfig = ReduceFunctionConfiguration<int>.Conf
                .Set(ReduceFunctionConfiguration<int>.ReduceFunction, GenericType<IntSumFunction>.Class)
                .Build();

            IConfiguration iteratorConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<NumIterations, int>(GenericType<NumIterations>.Class,
                    numIterations.ToString(CultureInfo.InvariantCulture))
               .Build();

            Func<string, IConfiguration> masterTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<BroadcastReduceMasterTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<DriverMessageHandler>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<BroadcastReduceMasterTask>.Class)
                    .Build())
                .BindNamedParameter<ElasticServiceConfigurationOptions.NumEvaluators, int>(
                    GenericType<ElasticServiceConfigurationOptions.NumEvaluators>.Class,
                    _numEvaluators.ToString(CultureInfo.InvariantCulture))
                .Build();

            Func<string, IConfiguration> slaveTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, taskId)
                        .Set(TaskConfiguration.Task, GenericType<BroadcastReduceSlaveTask>.Class)
                        .Set(TaskConfiguration.OnMessage, GenericType<DriverMessageHandler>.Class)
                        .Set(TaskConfiguration.OnClose, GenericType<BroadcastReduceSlaveTask>.Class)
                        .Build())
                    .Build();

            IElasticTaskSetSubscription subscription = _service.DefaultTaskSetSubscription();

            ElasticOperator pipeline = subscription.RootOperator;

            // Create and build the pipeline
            pipeline.Iterate(new DefaultFailureStateMachine(),
                        CheckpointLevel.None,
                        iteratorConfig)
                    .Reduce<int>(TopologyType.Flat)
                    .Broadcast<int>(TopologyType.Flat)
                    .Build();

            // Build the subscription
            _subscription = subscription.Build();

            // Create the task manager
            _taskManager = new DefaultTaskSetManager(_numEvaluators, _evaluatorRequestor, masterTaskConfiguration, slaveTaskConfiguration);

            // Register the subscription to the task manager
            _taskManager.AddTaskSetSubscription(_subscription);

            // Build the task set manager
            _taskManager.Build();
        }

        public void OnNext(IDriverStarted value)
        {
            var request = _evaluatorRequestor.NewBuilder()
                .SetNumber(_numEvaluators)
                .SetMegabytes(512)
                .SetCores(1)
                .SetRackName("WonderlandRack")
                .SetEvaluatorBatchId("IterateAllReduceEvaluator")
                .Build();
            _evaluatorRequestor.Submit(request);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            if (_taskManager.TryGetNextTaskContextId(allocatedEvaluator, out string identifier))
            {
                IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, identifier)
                    .Build();
                IConfiguration serviceConf = _service.GetServiceConfiguration();

                serviceConf = Configurations.Merge(serviceConf, _tcpPortProviderConfig, _codecConfig);
                allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
            }
            else
            {
                allocatedEvaluator.Dispose();
            }
        }

        public void OnNext(IActiveContext activeContext)
        {
            _taskManager.OnNewActiveContext(activeContext);
        }

        public void OnNext(IRunningTask value)
        {
            _taskManager.OnTaskRunning(value);
        }

        public void OnNext(ICompletedTask value)
        {
            _taskManager.OnTaskCompleted(value);

            if (_taskManager.IsCompleted())
            {
                _taskManager.Dispose();
            }
        }

        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            _taskManager.OnEvaluatorFailure(failedEvaluator);
        }

        public void OnNext(IFailedTask failedTask)
        {
            _taskManager.OnTaskFailure(failedTask);

            if (_taskManager.IsCompleted())
            {
                _taskManager.Dispose();
            }
        }

        public void OnNext(ITaskMessage taskMessage)
        {
            _taskManager.OnTaskMessage(taskMessage);
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }
    }
}
