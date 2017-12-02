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

using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Wake.Time.Event;
using System;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    public class Timeout
    {
        private readonly TimeoutType _type;
        private readonly IObserver<Alarm> _handler;
        private readonly long _timeout;
        private readonly string _id;

        public Timeout(long timeout, IObserver<Alarm> handler, TimeoutType type, string id = "")
        {
            if (handler == null)
            {
                throw new ArgumentNullException("handler");
            }
            _handler = handler;
            _timeout = timeout;
            _type = type;
            _id = id;
        }

        internal Alarm GetAlarm(long realTimeout)
        {
            switch (_type)
            {
                case TimeoutType.Taskset:
                    return new TasksetAlarm(realTimeout + _timeout, _handler);
                case TimeoutType.Operator:
                    return new OperatorAlarm(realTimeout + _timeout, _handler, _id);
                default:
                    throw new IllegalStateException("Timeout type not recognized");
            }
        }

        public enum TimeoutType : int
        {
            Taskset = 1,
            Operator = 2
        }
    }
}
