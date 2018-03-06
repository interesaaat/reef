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
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using System.Collections.Generic;
using Org.Apache.REEF.IO.FileSystem.Local;
using System.IO;
using Org.Apache.REEF.Tang.Formats;
using System.Linq;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Network.Elastic.Task.Impl;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    /// <summary>
    /// Example implementation of broadcasting using the elastic group communication service.
    /// </summary>
    public class ElasticBroadcastPDDriver :
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<IDriverStarted>,
        IObserver<IRunningTask>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedTask>,
        IObserver<ITaskMessage>
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
        private ElasticBroadcastPDDriver(
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(ElasticServiceConfigurationOptions.StartingPort))] int startingPort,
            [Parameter(typeof(ElasticServiceConfigurationOptions.PortRange))] int portRange,
            [Parameter(typeof(ModelFilePath))] string modelFilePath,
            [Parameter(typeof(PartitionedDatasetFilesPath))] ISet<string> partitionedFiles,
            IElasticTaskSetService service,
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

            _codecConfig = StreamingCodecConfiguration<int[]>.Conf
                .Set(StreamingCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                .Build();

            var serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<IFileDeSerializer<IEnumerable<string>>, StringSerializer>(GenericType<IFileDeSerializer<IEnumerable<string>>>.Class,
                    GenericType<StringSerializer>.Class)
                .Build();

            var serializerConfString = (new AvroConfigurationSerializer()).ToString(serializerConf);

            IPartitionedInputDataSet masterDataSet = TangFactory.GetTang()
                .NewInjector(FileSystemInputPartitionConfiguration<IEnumerable<string>>.ConfigurationModule
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<string>>.FilePathForPartitions, modelFilePath)
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<string>>.FileSerializerConfig, serializerConfString)
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<string>>.CopyToLocal, "true")
                .Build(),
                  LocalFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IPartitionedInputDataSet>();

            var inputDataSetConf = FileSystemInputPartitionConfiguration<IEnumerable<string>>.ConfigurationModule
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<string>>.FileSerializerConfig, serializerConfString)
                    .Set(FileSystemInputPartitionConfiguration<IEnumerable<string>>.CopyToLocal, "true");

            foreach (var partition in partitionedFiles)
            {
                inputDataSetConf = inputDataSetConf.Set(FileSystemInputPartitionConfiguration<IEnumerable<string>>.FilePathForPartitions, partition);
            }

            IPartitionedInputDataSet inputDataSet = TangFactory.GetTang()
                .NewInjector(inputDataSetConf.Build(),
                  LocalFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IPartitionedInputDataSet>();

            Func<string, IConfiguration> masterTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                Configurations.Merge(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<BroadcastPDMasterTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<DriverMessageHandler>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<BroadcastPDMasterTask>.Class)
                    .Build()), masterDataSet.First().GetPartitionConfiguration())
                .Build();

            Func<string, IConfiguration> slaveTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<BroadcastPDSlaveTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<DriverMessageHandler>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<BroadcastPDSlaveTask>.Class)
                    .Build())
                .Build();

            IElasticTaskSetSubscription subscription = _service.DefaultTaskSetSubscription();
            subscription.AddDataset(inputDataSet);

            ElasticOperator pipeline = subscription.RootOperator;

            // Create and build the pipeline
            pipeline.Broadcast<int[]>(TopologyType.Tree)
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
                .SetEvaluatorBatchId("BroadcastPDEvaluator")
                .Build();
            _evaluatorRequestor.Submit(request);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            string identifier = _taskManager.GetNextTaskContextId(allocatedEvaluator);

            IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, identifier)
                .Build();
            IConfiguration serviceConf = _service.GetServiceConfiguration();

            serviceConf = Configurations.Merge(serviceConf, _tcpPortProviderConfig, _codecConfig);
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
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

            if (_taskManager.IsDone())
            {
                _taskManager.Dispose();
            }
        }

        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            _taskManager.OnEvaluatorFailure(failedEvaluator);

            if (_taskManager.IsDone())
            {
                _taskManager.Dispose();
            }
        }

        public void OnNext(IFailedTask failedTask)
        {
            _taskManager.OnTaskFailure(failedTask);

            if (_taskManager.IsDone())
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

    [NamedParameter(Documentation = "Model file path")]
    public class ModelFilePath : Name<string>
    {
    }

    [NamedParameter(documentation: "Partitioned datasets paths")]
    public class PartitionedDatasetFilesPath : Name<ISet<string>>
    {
    }

    internal class StringSerializer : IFileDeSerializer<IEnumerable<string>>
    {
        [Inject]
        private StringSerializer()
        {
        }

        /// <summary>
        /// Read bytes from all the files in the set and return one by one
        /// </summary>
        /// <param name="filePaths"></param>
        /// <returns></returns>
        public IEnumerable<string> Deserialize(ISet<string> filePaths)
        {
            foreach (var f in filePaths)
            {
                using (FileStream stream = File.Open(f, FileMode.Open))
                {
                    StreamReader reader = new StreamReader(stream);
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        yield return line;
                    }
                }
            }
        }
    }
}
