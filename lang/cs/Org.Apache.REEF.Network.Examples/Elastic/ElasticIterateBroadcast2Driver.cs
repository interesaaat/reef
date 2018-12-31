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

using System;
using System.Globalization;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
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
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Enum;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    /// <summary>
    /// Example implementation of an iterative broadcast pipeline using the elastic group communication service.
    /// </summary>
    public class ElasticIterateBroadcast2Driver : 
        IObserver<IAllocatedEvaluator>, 
        IObserver<IActiveContext>, 
        IObserver<IDriverStarted>,
        IObserver<IRunningTask>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedTask>,
        IObserver<ITaskMessage>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticIterateBroadcast2Driver));

        private readonly int _numEvaluators;
        private readonly int _numIterations;

        private readonly IConfiguration _tcpPortProviderConfig;
        private readonly IConfiguration _codecConfig;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private readonly IElasticContext _context;
        private readonly IElasticStage _stage;
        private readonly IElasticTaskSetManager _taskManager;

        [Inject]
        private ElasticIterateBroadcast2Driver(
            [Parameter(typeof(OperatorParameters.NumIterations))] int numIterations,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(ElasticServiceConfigurationOptions.StartingPort))] int startingPort,
            [Parameter(typeof(ElasticServiceConfigurationOptions.PortRange))] int portRange,
            IElasticContext service,
            IEvaluatorRequestor evaluatorRequestor)
        {
            _numIterations = numIterations;
            _numEvaluators = numEvaluators;
            _context = service;
            _evaluatorRequestor = evaluatorRequestor;

            _tcpPortProviderConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class,
                    startingPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class,
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            _codecConfig = StreamingCodecConfiguration<byte[]>.Conf
                .Set(StreamingCodecConfiguration<byte[]>.Codec, GenericType<ByteArrayStreamingCodec>.Class)
                .Build();

            IConfiguration iteratorConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<OperatorParameters.NumIterations, int>(GenericType<OperatorParameters.NumIterations>.Class,
                    numIterations.ToString(CultureInfo.InvariantCulture))
               .Build();

            Func<string, IConfiguration> masterTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<IterateBroadcast2MasterTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<ElasticDriverMessageHandler>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<IterateBroadcast2MasterTask>.Class)
                    .Build())
                .BindNamedParameter<ElasticServiceConfigurationOptions.NumEvaluators, int>(
                    GenericType<ElasticServiceConfigurationOptions.NumEvaluators>.Class,
                    _numEvaluators.ToString(CultureInfo.InvariantCulture))
                .Build();

            Func<string, IConfiguration> slaveTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<IterateBroadcast2SlaveTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<ElasticDriverMessageHandler>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<IterateBroadcast2SlaveTask>.Class)
                    .Build())
                .Build();

            IElasticStage stage = _context.DefaultStage();

            ElasticOperator pipeline = stage.RootOperator;

            // Create and build the pipeline
            pipeline.Iterate(new DefaultFailureStateMachine(),
                        CheckpointLevel.None,
                        iteratorConfig)
                    .Broadcast<byte[]>(TopologyType.Tree,
                        CheckpointLevel.None)
                    .Iterate(new DefaultFailureStateMachine(),
                        CheckpointLevel.None,
                        iteratorConfig)
                    .Broadcast<byte[]>(TopologyType.Tree,
                        CheckpointLevel.None)
                    .Build();

            // Build the stage
            _stage = stage.Build();

            // Create the task manager
            _taskManager = _context.CreateNewTaskSetManager(masterTaskConfiguration, slaveTaskConfiguration);

            // Register the stage to the task manager
            _taskManager.AddStage(_stage);

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
                .SetEvaluatorBatchId("IterateBroadcastEvaluator")
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
                IConfiguration serviceConf = _context.GetElasticServiceConfiguration();

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
            LOGGER.Log(Level.Info, "Task {0} completed.", value.Id);
            _taskManager.OnTaskCompleted(value);

            if (_taskManager.IsCompleted())
            {
                LOGGER.Log(Level.Info, "TaskSet completed.");
                _taskManager.Dispose();
            }
        }

        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            _taskManager.OnEvaluatorFailure(failedEvaluator);

            if (_taskManager.IsCompleted())
            {
                _taskManager.Dispose();
            }
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
            _taskManager.Dispose();
        }

        public void OnError(Exception error)
        {
            _taskManager.Dispose();
        }
    }
}
