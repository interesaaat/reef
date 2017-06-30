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

namespace Org.Apache.REEF.Network.Examples.Elastic.Broadcast
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
        private readonly int _numRetry;

        private readonly IElasticTaskSetService _service;
        private readonly IConfiguration _tcpPortProviderConfig;
        private readonly IConfiguration _codecConfig;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        private ElasticBroadcastDriver(
            [Parameter(typeof(ElasticConfig.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(ElasticConfig.NumRetry))] int numRetry,
            [Parameter(typeof(ElasticConfig.StartingPort))] int startingPort,
            [Parameter(typeof(ElasticConfig.PortRange))] int portRange,
            ElasticTaskSetService service,
            IEvaluatorRequestor evaluatorRequestor)
        {
            _numEvaluators = numEvaluators;
            _numRetry = numRetry;
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

            // Create and build the pipeline (in this case composed by only one operator)
            pipeline.Broadcast(TopologyTypes.Tree,
                        PolicyLevel.Ignore,
                        dataConverterConfig)
                    .Build();

            // Build the subscription
            subscription.Build();
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            int id = -1;
            var subscriptions = _service.GetSubscriptions;
            IElasticTaskSetSubscription sub = null;

            while (id < 0 && subscriptions.MoveNext())
            {
                sub = subscriptions.Current;
                if (!sub.DoneWithContexts)
                {
                    id = sub.GetTaskContextId();
                }
            }

            if (sub == null || id < 0)
            {
                throw new IllegalStateException("Somethig went wrong while generating context configuration");
            }

            string identifier = Utils.GetTaskContextName(sub.GetSubscriptionName, id);

            IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, identifier)
                .Build();
            IConfiguration serviceConf = _service.GetServiceConfiguration();
            serviceConf = Configurations.Merge(serviceConf, _tcpPortProviderConfig, _codecConfig);
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            IConfiguration serviceConfig = _service.GetServiceConfiguration();

            bool isMaster = _service.IsMasterTaskContext(activeContext);

            var subscriptions = _service.GetSubscriptions;
            _commGroup.AddTask(GroupTestConstants.MasterTaskId);
            string taskId = Utils.BuildTaskId(s)

                           
            IConfiguration partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, GroupTestConstants.MasterTaskId)
                    .Set(TaskConfiguration.Task, GenericType<ITask>.Class)
                    .Build())
                .BindNamedParameter<ElasticConfig.NumEvaluators, int>(
                    GenericType<ElasticConfig.NumEvaluators>.Class,
                    _numEvaluators.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<ElasticConfig.NumRetry, int>(
                    GenericType<ElasticConfig.NumRetry>.Class,
                    _numRetry.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<ElasticConfig.IsMasterTask, bool>(
                    GenericType<ElasticConfig.IsMasterTask>.Class,
                    isMaster.ToString(CultureInfo.InvariantCulture))
                .Build();

            _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
        }

        public void OnNext(IDriverStarted value)
        {
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(_numEvaluators)
                    .SetMegabytes(512)
                    .SetCores(2)
                    .SetRackName("WonderlandRack")
                    .SetEvaluatorBatchId("BroadcastEvaluator")
                    .Build();
            _evaluatorRequestor.Submit(request);
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
