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
using System.Linq;
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
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    /// <summary>
    /// Example implementation of a parameter server using the elastic group communication service.
    /// </summary>
    public class ElasticParameterServerDriver : 
        IObserver<IAllocatedEvaluator>, 
        IObserver<IActiveContext>, 
        IObserver<IDriverStarted>,
        IObserver<IRunningTask>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedTask>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticParameterServerDriver));

        private readonly int _numEvaluators;
        private readonly int _numIterations;

        private readonly IConfiguration _tcpPortProviderConfig;
        private readonly IConfiguration _codecConfig;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private readonly IElasticTaskSetService _service;

        private readonly IElasticTaskSetSubscription _serversSubscription;

        private readonly IElasticTaskSetSubscription _serverA;
        private readonly IElasticTaskSetSubscription _serverB;
        private readonly IElasticTaskSetSubscription _serverC;

        private readonly ITaskSetManager _serversTaskManager;

        private readonly ITaskSetManager _workersTaskManager;

        [Inject]
        private ElasticParameterServerDriver(
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

            Func<string, IConfiguration> masterServerTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<HelloMasterTask>.Class)
                    .Build())
                .BindNamedParameter<ElasticServiceConfigurationOptions.NumServers, int>(
                    GenericType<ElasticServiceConfigurationOptions.NumServers>.Class,
                    3.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<ElasticServiceConfigurationOptions.NumWorkers, int>(
                    GenericType<ElasticServiceConfigurationOptions.NumWorkers>.Class,
                    6.ToString(CultureInfo.InvariantCulture))
                .Build();

            Func<string, IConfiguration> slaveServerTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<HelloServerTask>.Class)
                    .Build())
                .BindNamedParameter<ElasticServiceConfigurationOptions.NumWorkers, int>(
                    GenericType<ElasticServiceConfigurationOptions.NumWorkers>.Class,
                    6.ToString(CultureInfo.InvariantCulture))
                .Build();

            Func<string, IConfiguration> workerTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<HelloSlaveTask>.Class)
                    .Build())
                .Build();

            // Subscriptions
            IElasticTaskSetSubscription subscription = _service.NewTaskSetSubscription("servers", 3);

            ElasticOperator pipeline = subscription.RootOperator;

            pipeline.Iterate(iteratorConfig)
                    .Broadcast<int>(1, new FlatTopology(1))
                    .Build();

            _serversSubscription = subscription.Build();

            subscription = _service.NewTaskSetSubscription("server A", 7);

            pipeline = subscription.RootOperator;

            pipeline.Broadcast<int>(1, new TreeTopology(1, 2, true),
                        new DefaultFailureStateMachine(),
                        CheckpointLevel.None)
                    .Reduce<int>(1, TopologyType.Tree,
                        new DefaultFailureStateMachine(),
                        CheckpointLevel.None,
                        reduceFunctionConfig)
                    .Build();

            _serverA = subscription.Build();

            subscription = _service.NewTaskSetSubscription("server B", 7);

            pipeline = subscription.RootOperator;

            pipeline.Broadcast<int>(2, new TreeTopology(1, 2, true),
                        new DefaultFailureStateMachine(),
                        CheckpointLevel.None)
                     .Reduce<int>(2, TopologyType.Tree,
                        new DefaultFailureStateMachine(),
                        CheckpointLevel.None,
                        reduceFunctionConfig)
                    .Build();

            _serverB = subscription.Build();

            subscription = _service.NewTaskSetSubscription("server C", 7);

            pipeline = subscription.RootOperator;

            pipeline.Broadcast<int>(3, new TreeTopology(1, 2, true),
                        new DefaultFailureStateMachine(),
                        CheckpointLevel.None)
                    .Reduce<int>(3, TopologyType.Tree,
                        new DefaultFailureStateMachine(),
                        CheckpointLevel.None,
                        reduceFunctionConfig)
                    .Build();

            _serverC = subscription.Build();

            // Create the servers task manager
            _serversTaskManager = new DefaultTaskSetManager(3, _evaluatorRequestor, masterServerTaskConfiguration, slaveServerTaskConfiguration);

            // Register the subscriptions to the server task manager
            _serversTaskManager.AddTaskSetSubscription(_serversSubscription);
            _serversTaskManager.AddTaskSetSubscription(_serverA);
            _serversTaskManager.AddTaskSetSubscription(_serverB);
            _serversTaskManager.AddTaskSetSubscription(_serverC);

            // Create the workers task manager
            _workersTaskManager = new DefaultTaskSetManager(6, _evaluatorRequestor, workerTaskConfiguration);

            // Register the subscriptions to the workers task manager
            _workersTaskManager.AddTaskSetSubscription(_serverA);
            _workersTaskManager.AddTaskSetSubscription(_serverB);
            _workersTaskManager.AddTaskSetSubscription(_serverC);

            // Build the task set managers
            _serversTaskManager.Build();
            _workersTaskManager.Build();
        }

        public void OnNext(IDriverStarted value)
        {
            var request = _evaluatorRequestor.NewBuilder()
                .SetNumber(_numEvaluators)
                .SetMegabytes(512)
                .SetCores(1)
                .SetRackName("WonderlandRack")
                .SetEvaluatorBatchId("ParameterServer")
                .Build();
            _evaluatorRequestor.Submit(request);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            string identifier = null;

            if (_serversTaskManager.HasMoreContextToAdd())
            {
                _serversTaskManager.TryGetNextTaskContextId(allocatedEvaluator, out identifier);
            }
            else if (_workersTaskManager.HasMoreContextToAdd() && identifier == null)
            {
                _workersTaskManager.TryGetNextTaskContextId(allocatedEvaluator, out identifier);
            }

            if (identifier == null)
            {
                LOGGER.Log(Level.Warning, string.Format("Initializing a number of contexts different than configured"));
                allocatedEvaluator.Dispose();
            }

            IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, identifier)
                .Build();
            IConfiguration serviceConf = _service.GetServiceConfiguration();

            serviceConf = Configurations.Merge(serviceConf, _tcpPortProviderConfig, _codecConfig);
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            if (_serversTaskManager.IsContextManagedBy(activeContext.Id))
            {
                _serversTaskManager.OnNewActiveContext(activeContext);
            }
            else if (_workersTaskManager.IsContextManagedBy(activeContext.Id))
            {
                _workersTaskManager.OnNewActiveContext(activeContext);
            }
            else
            {
                throw new IllegalStateException(string.Format("Task manager for context {0} not found", activeContext.Id));
            }
        }

        public void OnNext(IRunningTask task)
        {
            if (_serversTaskManager.IsTaskManagedBy(task.Id))
            {
                _serversTaskManager.OnTaskRunning(task);
            }
            else if (_workersTaskManager.IsTaskManagedBy(task.Id))
            {
                _workersTaskManager.OnTaskRunning(task);
            }
            else
            {
                throw new IllegalStateException(string.Format("Task manager for task {0} not found", task.Id));
            }
        }

        public void OnNext(ICompletedTask task)
        {
            if (_serversTaskManager.IsTaskManagedBy(task.Id))
            {
                _serversTaskManager.OnTaskCompleted(task);
            }
            else if (_workersTaskManager.IsTaskManagedBy(task.Id))
            {
                _workersTaskManager.OnTaskCompleted(task);
            }
            else
            {
                throw new IllegalStateException(string.Format("Task manager for task {0} not found", task.Id));
            }
            
            if (_serversTaskManager.IsCompleted())
            {
                _serversTaskManager.Dispose();
            }

            if (_workersTaskManager.IsCompleted())
            {
                _workersTaskManager.Dispose();
            }
        }

        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            if (_serversTaskManager.IsEvaluatorManagedBy(failedEvaluator.Id))
            {
                _serversTaskManager.OnEvaluatorFailure(failedEvaluator);
            }
            else if (_workersTaskManager.IsEvaluatorManagedBy(failedEvaluator.Id))
            {
                _workersTaskManager.OnEvaluatorFailure(failedEvaluator);
            }
            else
            {
                throw new IllegalStateException(string.Format("Task manager for evaluator {0} not found", failedEvaluator.Id));
            }

            if (_serversTaskManager.IsCompleted())
            {
                _serversTaskManager.Dispose();
            }

            if (_workersTaskManager.IsCompleted())
            {
                _workersTaskManager.Dispose();
            }
        }

        public void OnNext(IFailedTask task)
        {
            if (_serversTaskManager.IsTaskManagedBy(task.Id))
            {
                _serversTaskManager.OnTaskFailure(task);
            }
            else if (_workersTaskManager.IsTaskManagedBy(task.Id))
            {
                _workersTaskManager.OnTaskFailure(task);
            }
            else
            {
                throw new IllegalStateException(string.Format("Task manager for task {0} not found", task.Id));
            }

            if (_serversTaskManager.IsCompleted())
            {
                _serversTaskManager.Dispose();
            }

            if (_workersTaskManager.IsCompleted())
            {
                _workersTaskManager.Dispose();
            }
        }

        public void OnCompleted()
        {
            _serversTaskManager.Dispose();

            _workersTaskManager.Dispose();
        }

        public void OnError(Exception error)
        {
            _serversTaskManager.Dispose();

            _workersTaskManager.Dispose();
        }
    }
}
