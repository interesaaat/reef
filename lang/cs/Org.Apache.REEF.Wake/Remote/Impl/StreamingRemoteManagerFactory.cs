﻿/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System.Net;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// StreamingRemoteManagerFactory for StreamingRemoteManager.
    /// </summary>
    public sealed class StreamingRemoteManagerFactory
    {
        private readonly ITcpPortProvider _tcpPortProvider;
        private readonly IInjector _injector;

        [Inject]  
        private StreamingRemoteManagerFactory(ITcpPortProvider tcpPortProvider, IInjector injector)
        {
            _tcpPortProvider = tcpPortProvider;
            _injector = injector;
        }

        public IRemoteManager<T> GetInstance<T>(IPAddress localAddress, IStreamingCodec<T> codec)
        {
#pragma warning disable 618
// This is the one place allowed to call this constructor. Hence, disabling the warning is OK.
            return new StreamingRemoteManager<T>(localAddress, _tcpPortProvider, codec);
#pragma warning disable 618
        }
    }
}