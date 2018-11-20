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

using Org.Apache.REEF.Wake.Time.Event;
using System;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    public class OperatorTimeout : ITimeout
    {
        private readonly IObserver<Alarm> _handler;
        private readonly long _timeout;
        private readonly string _id;

        public OperatorTimeout(long timeout, IObserver<Alarm> handler, string id = "")
        {
            _handler = handler ?? throw new ArgumentNullException(nameof(handler));
            _timeout = timeout;
            _id = id;
        }

        public Alarm GetAlarm(long realTimeout)
        {
            return new OperatorAlarm(realTimeout + _timeout, _handler, _id);
        }
    }
}
