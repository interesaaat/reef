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

/// <summary>
/// The default implementation for IFailureState.
/// This events are generated based on the default failure states.
/// </summary>
namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    public class DefaultFailureState : IFailureState
    {
        public DefaultFailureState()
        {
            FailureState = (int)DefaultFailureStates.Continue;
        }

        public DefaultFailureState(int state)
        {
            FailureState = state;
        }

        public int FailureState { get; set; }

        public IFailureState Merge(IFailureState that)
        {
            return new DefaultFailureState(Math.Max(FailureState, that.FailureState));
        }
    }

    public enum DefaultFailureStates : int
    {
        Continue = 1,

        ContinueAndReconfigure = 2,

        ContinueAndReschedule = 3,

        StopAndReschedule = 4,

        Fail = 5
    }

    public enum DefaultSubscriptionStates : int
    {
        Go = 0,

        Stop = 1
    }
}
