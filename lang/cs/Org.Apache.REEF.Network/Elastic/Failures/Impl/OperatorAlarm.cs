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

using Org.Apache.REEF.Wake.Time.Event;
using System;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    public sealed class OperatorAlarm : Alarm
    {
        /// <summary>
        /// Class representing an alarm triggered by an elastic operator.
        /// </summary>
        /// <param name="timeout">The amout of time that have to elaps in order to trigger the alarm</param>
        /// <param name="handler"></param>
        /// <param name="id"></param>
        public OperatorAlarm(long timeout, IObserver<Alarm> handler, string id) : base(timeout, handler)
        {
            Id = id;
        }

        public string Id { get; private set; }
    }
}
