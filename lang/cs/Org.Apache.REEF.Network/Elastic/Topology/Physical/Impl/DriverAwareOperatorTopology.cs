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

using Org.Apache.REEF.Network.Elastic.Comm;
using System;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal abstract class DriverAwareOperatorTopology : OperatorTopology, IObserver<DriverMessagePayload>
    {
        internal DriverAwareOperatorTopology(string taskId, int rootId, string subscription, int operatorId)
            : base(taskId, rootId, subscription, operatorId)
        {
        }

        public void OnNext(DriverMessagePayload message)
        {
            switch (message.PayloadType)
            {
                case DriverMessagePayloadType.Ring:
                case DriverMessagePayloadType.Topology:
                    OnMessageFromDriver(message);
                    break;
                default:
                    OnFailureResponseMessageFromDriver(message);
                    break;
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        internal abstract void OnMessageFromDriver(DriverMessagePayload value);

        internal abstract void OnFailureResponseMessageFromDriver(DriverMessagePayload value);
    }
}
