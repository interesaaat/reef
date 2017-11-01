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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Config;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    class TaskSetManagerParameters
    {
        [Inject]
        public TaskSetManagerParameters(
            [Parameter(typeof(ElasticServiceConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(ElasticServiceConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumTaskFailures))] int numFailures,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorRackName))] string rackName,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorBatchId))] string batchId,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorNumCores))] int numCores,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorMemorySize))] int memorySize)
        {
            Timeout = timeout;
            Retry = retry;
            NumTaskFailures = numFailures;
            NewEvaluatorRackName = rackName;
            NewEvaluatorBatchId = batchId;
            NewEvaluatorNumCores = numCores;
            NewEvaluatorMemorySize = memorySize;
        }

        internal int Timeout { get; private set; }

        internal int Retry { get; private set; }

        internal int NumTaskFailures { get; private set; }

        internal string NewEvaluatorRackName { get; private set; }

        internal string NewEvaluatorBatchId { get; private set; }

        internal int NewEvaluatorNumCores { get; private set; }

        internal int NewEvaluatorMemorySize { get; private set; }
    }
}
