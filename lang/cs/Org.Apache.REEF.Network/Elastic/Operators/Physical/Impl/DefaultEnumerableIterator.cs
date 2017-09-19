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

using System.Threading;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Collections;
using System;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Default Group Communication Operator used to iterate over a fixed set of ints.
    /// </summary>
    public sealed class DefaultEnumerableIterator : IElasticTypedIterator<int>
    {
        private readonly ElasticIteratorEnumerator<int> _inner;

        /// <summary>
        /// Creates a new Enumerable Iterator.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="innerIterator">The inner enumerator implementing the iterator</param>
        [Inject]
        private DefaultEnumerableIterator(
            [Parameter(typeof(OperatorParameters.OperatorId))] int id,
            ForLoopEnumerator innerIterator)
        {
            OperatorName = Constants.Iterate;
            OperatorId = id;
            _inner = innerIterator;
        }

        public int OperatorId { get; private set; }

        public string OperatorName { get; private set; }

        public int Current
        {
            get { return _inner.Current; }
        }

        object IEnumerator.Current
        {
            get { return _inner.Current; }
        }

        object IElasticIterator.Current
        {
            get { return _inner.Current; }
        }

        public string FailureInfo
        {
            get { return PositionTracker.Nil.ToString(); }
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
        }

        public void WaitCompletionBeforeDisposing()
        {
            return;
        }

        public bool MoveNext()
        {
            return _inner.MoveNext();
        }

        public void Reset()
        {
            _inner.Reset();
        }

        public void Dispose()
        {
            _inner.Dispose();
        }
    }
}
