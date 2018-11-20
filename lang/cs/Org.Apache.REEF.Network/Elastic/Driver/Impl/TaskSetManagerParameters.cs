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
using Org.Apache.REEF.Wake.Time;
using System;
using Org.Apache.REEF.Wake.Time.Event;
using System.Threading.Tasks;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    class TaskSetManagerParameters
    {
        private FailuresClock _clock;

        [Inject]
        public TaskSetManagerParameters(
            FailuresClock clock,
            [Parameter(typeof(ElasticServiceConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(ElasticServiceConfigurationOptions.SendRetry))] int retry,
            [Parameter(typeof(ElasticServiceConfigurationOptions.RetryWaitTime))] int waitTime,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumTaskFailures))] int numFailures,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorRackName))] string rackName,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorBatchId))] string batchId,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorNumCores))] int numCores,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorMemorySize))] int memorySize)
        {
            _clock = clock;
            Timeout = timeout;
            Retry = retry;
            WaitTime = waitTime;
            NumTaskFailures = numFailures;
            NewEvaluatorRackName = rackName;
            NewEvaluatorBatchId = batchId;
            NewEvaluatorNumCores = numCores;
            NewEvaluatorMemorySize = memorySize;

            System.Threading.Tasks.Task.Factory.StartNew(() => _clock.Run(), TaskCreationOptions.LongRunning);
        }

        internal int Timeout { get; private set; }

        internal int Retry { get; private set; }

        internal int WaitTime { get; private set; }

        internal int NumTaskFailures { get; private set; }

        internal string NewEvaluatorRackName { get; private set; }

        internal string NewEvaluatorBatchId { get; private set; }

        internal int NewEvaluatorNumCores { get; private set; }

        internal int NewEvaluatorMemorySize { get; private set; }

        internal void ScheduleAlarm(long timeout, IObserver<Alarm> alarm)
        {
            _clock.ScheduleAlarm(timeout, alarm);
        }

        internal void ScheduleAlarm(ITimeout timeout)
        {
            _clock.ScheduleAlarm(timeout);
        }
    }
}
