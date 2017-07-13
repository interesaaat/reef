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

/// <summary>
/// The default implementation of the failure events.
/// </summary>
namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    public class DefaultFailureEvent : IFailureEvent
    {
        public DefaultFailureEvent()
        {
            FailureEvent = (int)DefaultFailureStateEvents.Continue;
        }

        public DefaultFailureEvent(int @event)
        {
            FailureEvent = @event;
        }

        public int FailureEvent { get; set; }

        public void Dispose()
        {
        }
    }

    public enum DefaultFailureStateEvents : int
    {
        Continue = 1,

        Reconfigure = 2,

        Reschedule = 3,

        Stop = 4
    }
}
