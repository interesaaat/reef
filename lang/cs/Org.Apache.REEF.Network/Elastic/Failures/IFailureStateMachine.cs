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

using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Tang.Annotations;
using System;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Where the decision is made on what to do when a failure happen.
    /// A decision is made based on how many data points are lost.
    /// </summary>
    [DefaultImplementation(typeof(DefaultFailureStateMachine))]
    public interface IFailureStateMachine
    {
        IFailureState AddDataPoints(int points);

        IFailureState RemoveDataPoints(int points);

        IFailureStateMachine Clone();

        IFailureStateMachine Build();

        IFailureState State { get; }

        void SetThreashold(IFailureState level, float threshold);

        void SetThreasholds(params Tuple<IFailureState, float>[] weights);

        int NumOfDataPoints { get; set; }

        int NumOfFailedDataPoints { get; set; }
    }
}
