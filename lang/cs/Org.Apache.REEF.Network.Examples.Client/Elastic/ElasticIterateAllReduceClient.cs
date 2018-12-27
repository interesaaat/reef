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

namespace Org.Apache.REEF.Network.Examples.Client.Elastic
{
    public class ElasticIterateAllReduceClient
    {
        const string Local = "local";
        const string Yarn = "yarn";
        const string DefaultRuntimeFolder = "REEF_LOCAL_RUNTIME";

        public void RunIterateAllReduce(bool runOnYarn, int numTasks, int startingPortNo, int portRange)
        {
            const string driverId = "ElasticIterativeAllReduceDriver";
            const string stage = "IterativeAllReduce";

            IConfiguration driverConfig = TangFactory.GetTang().NewConfigurationBuilder(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<ElasticIterateAllReduceDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ElasticIterateAllReduceDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<ElasticIterateAllReduceDriver>.Class)
                    .Set(DriverConfiguration.OnContextActive, GenericType<ElasticIterateAllReduceDriver>.Class)
                    .Set(DriverConfiguration.OnTaskRunning, GenericType<ElasticIterateAllReduceDriver>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted, GenericType<ElasticIterateAllReduceDriver>.Class)
                    .Set(DriverConfiguration.OnTaskFailed, GenericType<ElasticIterateAllReduceDriver>.Class)
                    .Set(DriverConfiguration.OnTaskMessage, GenericType<ElasticIterateAllReduceDriver>.Class)
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
                .Build();

            IConfiguration groupCommDriverConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<ElasticServiceConfigurationOptions.DriverId>(driverId)
                .BindStringNamedParam<ElasticServiceConfigurationOptions.DefaultStageName>(stage)
                .BindIntNamedParam<ElasticServiceConfigurationOptions.NumberOfTasks>(numTasks.ToString(CultureInfo.InvariantCulture))
                .Build();

            IConfiguration merged = Configurations.Merge(driverConfig, groupCommDriverConfig);

            string runPlatform = runOnYarn ? "yarn" : "local";
            TestRun(merged, typeof(ElasticIterateAllReduceDriver), numTasks, "IA", runPlatform);
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
                    return YARNClientConfiguration.ConfigurationModule.Build();
                default:
                    throw new Exception("Unknown runtime: " + runOnYarn);
            }
        }
    }
}
