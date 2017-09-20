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
using Org.Apache.REEF.Network.Elastic;
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

            IConfiguration reduceFunctionConfig = ReduceFunctionConfiguration<int, int>.Conf
                .Set(ReduceFunctionConfiguration<int, int>.ReduceFunction, GenericType<SumFunction>.Class)
                .Build();

            IConfiguration iteratorConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<NumIterations, int>(GenericType<NumIterations>.Class,
                    numIterations.ToString(CultureInfo.InvariantCulture))
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
                        Network.Elastic.Failures.CheckpointLevel.None)
                    .Reduce(1, TopologyType.Tree,
                        new DefaultFailureStateMachine(),
                        Network.Elastic.Failures.CheckpointLevel.None,
                        reduceFunctionConfig)
                    .Build();

            _serverA = subscription.Build();

            subscription = _service.NewTaskSetSubscription("server B", 7);

            pipeline = subscription.RootOperator;

            pipeline.Broadcast<int>(2, new TreeTopology(1, 2, true),
                        new DefaultFailureStateMachine(),
                        Network.Elastic.Failures.CheckpointLevel.None)
                     .Reduce(2, TopologyType.Tree,
                        new DefaultFailureStateMachine(),
                        Network.Elastic.Failures.CheckpointLevel.None,
                        reduceFunctionConfig)
                    .Build();

            _serverB = subscription.Build();

            subscription = _service.NewTaskSetSubscription("server C", 7);

            pipeline = subscription.RootOperator;

            pipeline.Broadcast<int>(3, new TreeTopology(1, 2, true),
                        new DefaultFailureStateMachine(),
                        Network.Elastic.Failures.CheckpointLevel.None)
                    .Reduce(3, TopologyType.Tree,
                        new DefaultFailureStateMachine(),
                        Network.Elastic.Failures.CheckpointLevel.None,
                        reduceFunctionConfig)
                    .Build();

            _serverC = subscription.Build();

            // Create the servers task manager
            _serversTaskManager = new DefaultTaskSetManager(3);

            // Register the subscriptions to the server task manager
            _serversTaskManager.AddTaskSetSubscription(_serversSubscription);
            _serversTaskManager.AddTaskSetSubscription(_serverA);
            _serversTaskManager.AddTaskSetSubscription(_serverB);
            _serversTaskManager.AddTaskSetSubscription(_serverC);

            // Create the workers task manager
            _workersTaskManager = new DefaultTaskSetManager(6);

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
                identifier = _serversTaskManager.GetNextTaskContextId(allocatedEvaluator);
            }
            else if (_workersTaskManager.HasMoreContextToAdd())
            {
                identifier = _workersTaskManager.GetNextTaskContextId(allocatedEvaluator);
            }
            else
            {
                throw new IllegalStateException("Initializing a number of contexts different than configured");
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
            string taskId;
            IConfiguration partialTaskConf;

            bool isServerContext = _serversTaskManager.SubscriptionsId == _service.GetContextSubscriptions(activeContext);

            if (isServerContext)
            {
                taskId = _serversTaskManager.GetNextTaskId(activeContext);
                var servers = _serversTaskManager.IsMasterTaskContext(activeContext);

                if (servers.Any(subs => subs.SubscriptionName == "servers"))
                {
                    partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
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
                }
                else
                {
                    partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                      TaskConfiguration.ConfigurationModule
                          .Set(TaskConfiguration.Identifier, taskId)
                          .Set(TaskConfiguration.Task, GenericType<HelloServerTask>.Class)
                          .Build())
                      .BindNamedParameter<ElasticServiceConfigurationOptions.NumWorkers, int>(
                           GenericType<ElasticServiceConfigurationOptions.NumWorkers>.Class,
                           6.ToString(CultureInfo.InvariantCulture))
                      .Build();
                }

                _serversTaskManager.AddTask(taskId, partialTaskConf, activeContext);
            }
            else
            {
                taskId = _workersTaskManager.GetNextTaskId(activeContext);

                partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, taskId)
                        .Set(TaskConfiguration.Task, GenericType<HelloSlaveTask>.Class)
                        .Build())
                    .Build();

                _workersTaskManager.AddTask(taskId, partialTaskConf, activeContext);
            }
        }

        public void OnNext(IRunningTask value)
        {
            _serversTaskManager.OnTaskRunning(value);

            _workersTaskManager.OnTaskRunning(value);
        }

        public void OnNext(ICompletedTask value)
        {
            _serversTaskManager.OnTaskCompleted(value);

            _workersTaskManager.OnTaskCompleted(value);

            if (_serversTaskManager.Done())
            {
                _serversTaskManager.Dispose();
            }

            if (_workersTaskManager.Done())
            {
                _workersTaskManager.Dispose();
            }
        }

        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            _serversTaskManager.OnEvaluatorFailure(failedEvaluator);

            _workersTaskManager.OnEvaluatorFailure(failedEvaluator);

            if (_serversTaskManager.Done())
            {
                _serversTaskManager.Dispose();
            }

            if (_workersTaskManager.Done())
            {
                _workersTaskManager.Dispose();
            }
        }

        public void OnNext(IFailedTask failedTask)
        {
            _serversTaskManager.OnTaskFailure(failedTask);

            _workersTaskManager.OnTaskFailure(failedTask);

            if (_serversTaskManager.Done())
            {
                _serversTaskManager.Dispose();
            }

            if (_workersTaskManager.Done())
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
