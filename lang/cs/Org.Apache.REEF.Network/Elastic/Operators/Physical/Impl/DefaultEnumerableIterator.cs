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

using System.Threading;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Topology.Task.Impl;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using System;
using System.Collections;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Group Communication Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public sealed class DefaultEnumerableIterator<T> : IElasticIterator<T>
    {
        private readonly ElasticIteratorEnumerator<T> _inner;

        /// <summary>
        /// Creates a new BroadcastReceiver.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="commLayer">The node's communication layer graph</param>
        [Inject]
        private DefaultEnumerableIterator(
            [Parameter(typeof(OperatorsConfiguration.OperatorId))] int id,
            IInjector injector)
        {
            OperatorName = Constants.Iterate;
            OperatorId = id;
            switch (Type.GetTypeCode(typeof(T)))
            {
                case TypeCode.Int32:
                    var subInjector = injector.ForkInjector();
                    _inner = subInjector.GetInstance<ForLoopEnumerator>() as ElasticIteratorEnumerator<T>;
                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// Returns the operator identifier.
        /// </summary>
        public int OperatorId { get; private set; }

        public string OperatorName { get; private set; }

        public T Current
        {
            get { return _inner.Current; }
        }

        object IEnumerator.Current
        {
            get { return _inner.Current; }
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
        }

        public void Dispose()
        {
            _inner.Dispose();
        }

        public bool MoveNext()
        {
            return _inner.MoveNext();
        }

        public void Reset()
        {
            _inner.Reset();
        }
    }
}
