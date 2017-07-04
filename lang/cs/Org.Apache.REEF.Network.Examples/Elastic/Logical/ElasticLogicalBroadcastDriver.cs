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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Topology;
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
using Org.Apache.REEF.Network.Elastic.Driver.Policy;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Common.Context;

namespace Org.Apache.REEF.Network.Examples.Elastic.Logical
{
    public class ElasticBroadcastDriver : 
        IObserver<IAllocatedEvaluator>, 
        IObserver<IActiveContext>, 
        IObserver<IDriverStarted>,
        IObserver<IFailedEvaluator>, 
        IObserver<IFailedTask>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticBroadcastDriver));

        private readonly int _numEvaluators;

        private readonly IConfiguration _tcpPortProviderConfig;
        private readonly IConfiguration _codecConfig;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private readonly IElasticTaskSetService _service;
        private readonly IElasticTaskSetSubscription _subscription;
        private readonly ITaskSetManager _taskManager;

        [Inject]
        private ElasticBroadcastDriver(
            [Parameter(typeof(ElasticConfig.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(ElasticConfig.StartingPort))] int startingPort,
            [Parameter(typeof(ElasticConfig.PortRange))] int portRange,
            ElasticTaskSetService service,
            IEvaluatorRequestor evaluatorRequestor)
        {
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

            IConfiguration dataConverterConfig = PipelineDataConverterConfiguration<int>.Conf
                .Set(PipelineDataConverterConfiguration<int>.DataConverter, GenericType<DefaultPipelineDataConverter<int>>.Class)
                .Build();

            IElasticTaskSetSubscription subscription = _service.DefaultElasticTaskSetSubscription;

            ElasticOperator pipeline = subscription.GetRootOperator;

            // Create and build the pipeline
            pipeline.Broadcast(TopologyTypes.Tree,
                        PolicyLevel.Ignore,
                        dataConverterConfig)
                    .Build();

            // Build the subscription
            _subscription = subscription.Build();

            // Create the task manager
            _taskManager = new TaskSetManager(_numEvaluators);

            // Register the subscription to the task manager
            _taskManager.AddTaskSetSubscription(_subscription);
        }

        public void OnNext(IDriverStarted value)
        {
            var request = _evaluatorRequestor.NewBuilder()
                .SetNumber(_numEvaluators)
                .SetMegabytes(512)
                .SetCores(1)
                .SetRackName("WonderlandRack")
                .SetEvaluatorBatchId("BroadcastEvaluator")
                .Build();
            _evaluatorRequestor.Submit(request);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            int id = _taskManager.GetNextTaskContextId(allocatedEvaluator);
            string identifier = Utils.BuildContextName(_taskManager.GetSubscriptionsId, id);

            IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, identifier)
                .Build();
            IConfiguration serviceConf = _service.GetServiceConfiguration();

            serviceConf = Configurations.Merge(serviceConf, _tcpPortProviderConfig, _codecConfig);
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            bool isMaster = _taskManager.IsMasterTaskContext(activeContext);
            int id = _taskManager.GetNextTaskId(activeContext);
            string taskId = Utils.BuildTaskId(_taskManager.GetSubscriptionsId, id);

            IConfiguration partialTaskConf;

            if (isMaster)
            {
                partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, taskId)
                        .Set(TaskConfiguration.Task, GenericType<HelloMasterTask>.Class)
                        .Build())
                    .BindNamedParameter<ElasticConfig.NumEvaluators, int>(
                        GenericType<ElasticConfig.NumEvaluators>.Class,
                        _numEvaluators.ToString(CultureInfo.InvariantCulture))
                    .Build();
            }
            else
            {
                partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, taskId)
                        .Set(TaskConfiguration.Task, GenericType<HelloSlaveTask>.Class)
                        .Build())
                    .BindNamedParameter<ElasticConfig.NumEvaluators, int>(
                        GenericType<ElasticConfig.NumEvaluators>.Class,
                        _numEvaluators.ToString(CultureInfo.InvariantCulture))
                    .Build();
            }

            _taskManager.AddTask(taskId, partialTaskConf, activeContext);
        }

        public void OnNext(IFailedEvaluator value)
        {
            throw new NotImplementedException();
        }

        public void OnNext(IFailedTask value)
        {
            throw new NotImplementedException();
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
