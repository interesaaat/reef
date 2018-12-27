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
using System.IO;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Examples.Elastic;
using System.Collections.Generic;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.IO.FileSystem;

namespace Org.Apache.REEF.Network.Examples.Client.Elastic
{
    public class ElasticBroadcastClientPD
    {
        const string Local = "local";
        const string Yarn = "yarn";
        const string DefaultRuntimeFolder = "REEF_LOCAL_RUNTIME";

        public void RunElasticBroadcast(bool runOnYarn, int numTasks, int startingPortNo, int portRange)
        {
            const string driverId = "ElasticBroadcastClientPD";
            const string stage = "Broadcast";

            string modelFilePath = MakeLocalTempFile(new string[] { "1", "2", "3", "4", "5" });
            string partitionFilePath1 = MakeLocalTempFile(new string[] { "1", "2", "3", "4", "5", "6" });
            string partitionFilePath2 = MakeLocalTempFile(new string[] { "1", "2", "3", "4", "5", "7" });

            if (runOnYarn)
            {
                modelFilePath = MakeRemoteTestFile(modelFilePath);
                partitionFilePath1 = MakeRemoteTestFile(partitionFilePath1);
                partitionFilePath2 = MakeRemoteTestFile(partitionFilePath2);
            }

            ICsConfigurationBuilder driverConfigBuilder = TangFactory.GetTang().NewConfigurationBuilder(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<ElasticBroadcastPDDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ElasticBroadcastPDDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<ElasticBroadcastPDDriver>.Class)
                    .Set(DriverConfiguration.OnContextActive, GenericType<ElasticBroadcastPDDriver>.Class)
                    .Set(DriverConfiguration.OnTaskRunning, GenericType<ElasticBroadcastPDDriver>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted, GenericType<ElasticBroadcastPDDriver>.Class)
                    .Set(DriverConfiguration.OnTaskFailed, GenericType<ElasticBroadcastPDDriver>.Class)
                    .Set(DriverConfiguration.OnTaskMessage, GenericType<ElasticBroadcastPDDriver>.Class)
                    .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                    .Build())
                .BindNamedParameter<ElasticServiceConfigurationOptions.NumEvaluators, int>(
                    GenericType<ElasticServiceConfigurationOptions.NumEvaluators>.Class,
                    numTasks.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<ElasticServiceConfigurationOptions.StartingPort, int>(
                    GenericType<ElasticServiceConfigurationOptions.StartingPort>.Class,
                    startingPortNo.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<ElasticServiceConfigurationOptions.PortRange, int>(
                    GenericType<ElasticServiceConfigurationOptions.PortRange>.Class,
                    portRange.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<ModelFilePath, string>(
                    GenericType<ModelFilePath>.Class, modelFilePath)
                .BindSetEntry<PartitionedDatasetFilesPath, string>(
                    GenericType<PartitionedDatasetFilesPath>.Class, partitionFilePath1)
                .BindSetEntry<PartitionedDatasetFilesPath, string>(
                    GenericType<PartitionedDatasetFilesPath>.Class, partitionFilePath2);

            IConfiguration driverConfig = driverConfigBuilder.Build();

            IConfiguration groupCommDriverConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<ElasticServiceConfigurationOptions.DriverId>(driverId)
                .BindStringNamedParam<ElasticServiceConfigurationOptions.DefaultStageName>(stage)
                .BindIntNamedParam<ElasticServiceConfigurationOptions.NumberOfTasks>(numTasks.ToString(CultureInfo.InvariantCulture))
                .Build();

            IConfiguration merged = Configurations.Merge(driverConfig, groupCommDriverConfig);

            string runPlatform = runOnYarn ? "yarn" : "local";
            TestRun(merged, typeof(ElasticBroadcastPDDriver), numTasks, "ElasticBroadcastDriver", runPlatform);
        }

        internal static void TestRun(IConfiguration driverConfig, Type globalAssemblyType, int numberOfEvaluator, string jobIdentifier = "myDriver", string runOnYarn = "local", string runtimeFolder = DefaultRuntimeFolder)
        {
            IInjector injector = TangFactory.GetTang().NewInjector(GetRuntimeConfiguration(runOnYarn, numberOfEvaluator, runtimeFolder));
            var reefClient = injector.GetInstance<IREEFClient>();
            var jobRequestBuilder = injector.GetInstance<JobRequestBuilder>();
            var jobSubmission = jobRequestBuilder
                .AddDriverConfiguration(driverConfig)
                .AddGlobalAssemblyForType(globalAssemblyType)
                .SetJobIdentifier(jobIdentifier)
                .Build();

            reefClient.SubmitAndGetJobStatus(jobSubmission);
        }

        internal static IConfiguration GetRuntimeConfiguration(string runOnYarn, int numberOfEvaluator, string runtimeFolder)
        {
            switch (runOnYarn)
            {
                case Local:
                    var dir = Path.Combine(".", runtimeFolder);
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, numberOfEvaluator.ToString())
                        .Set(LocalRuntimeClientConfiguration.RuntimeFolder, dir)
                        .Build();
                case Yarn:
                    return Configurations.Merge(HadoopFileSystemConfiguration.ConfigurationModule.Build(), YARNClientConfiguration.ConfigurationModule.Build());
                default:
                    throw new Exception("Unknown runtime: " + runOnYarn);
            }
        }

        private static void MakeLocalTestFile(string filePath, string[] strings)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            using (var file = File.Create(filePath))
            using (var stream = new StreamWriter(file))
            {
                foreach (var s in strings)
                {
                    stream.WriteLine(s);
                }
            }
        }

        private static string MakeLocalTempFile(string[] bytes)
        {
            var result = Path.GetTempFileName();
            MakeLocalTestFile(result, bytes);
            return result;
        }

        private static string MakeRemoteTestFile(string localTempFile)
        {
            IFileSystem fileSystem =
                TangFactory.GetTang()
                .NewInjector(HadoopFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();

            string remoteFileName = "/tmp/TestHadoopFilePartition-" +
                                    DateTime.Now.ToString("yyyyMMddHHmmssfff");

            var remoteUri = fileSystem.CreateUriForPath(remoteFileName);
            Console.WriteLine("remoteUri {0}: ", remoteUri);

            fileSystem.CopyFromLocal(localTempFile, remoteUri);
            Console.WriteLine("File CopyFromLocal {0}: ", localTempFile);

            return remoteFileName;
        }
    }
}
