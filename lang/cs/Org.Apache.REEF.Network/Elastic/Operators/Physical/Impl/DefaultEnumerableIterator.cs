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
using System.Collections;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Default Group Communication Operator used to iterate over a fixed set of ints.
    /// </summary>
    public sealed class DefaultEnumerableIterator : ICheckpointingOperator, IElasticTypedIterator<int>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultEnumerableIterator));

        private readonly ElasticIteratorEnumerator<int> _inner;
        private readonly IterateTopology _topology;
        private ICheckpointableState _checkpointState;

        /// <summary>
        /// Creates a new Enumerable Iterator.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="innerIterator">The inner enumerator implementing the iterator</param>
        [Inject]
        private DefaultEnumerableIterator(
            [Parameter(typeof(OperatorId))] int id,
            ForLoopEnumerator innerIterator,
            ICheckpointableState state,
            IterateTopology topology)
        {
            OperatorName = Constants.Iterate;
            OperatorId = id;
            _checkpointState = state;
            _inner = innerIterator;
            _topology = topology;
        }

        public int OperatorId { get; private set; }

        public string OperatorName { get; private set; }

        public int Current
        {
            get { return _inner.Current; }
        }

        public string FailureInfo
        {
            get { return PositionTracker.Nil.ToString(); }
        }

        public ICheckpointableState CheckpointState
        {
            get
            {
                // Check if the state have to be resumed from a checkpoint
                if (_inner.IsStart && _inner.Current != 0)
                {
                    var checkpoint = _topology.GetCheckpoint(_inner.Current);
                    if (_inner.Current < 0)
                    {
                        LOGGER.Log(Level.Info, "Fast forward to iteration {0}", checkpoint.Iteration + 1);
                        while (_inner.Current < checkpoint.Iteration)
                        {
                            _inner.MoveNext();
                        }
                    }
                    _checkpointState.MakeCheckpointable(checkpoint.State);
                }
                return _checkpointState;
            }
            set
            {
                _checkpointState = value;
            }
        }

        public IElasticIterator IteratorReference { private get; set; }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        object IElasticIterator.Current
        {
            get { return Current; }
        }

        public void ResetPosition()
        {
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
        }

        public void WaitCompletionBeforeDisposing()
        {
        }

        public bool MoveNext()
        {
            if (_inner.MoveNext())
            {
                _topology.IterationNumber(Current);

                Checkpoint();

                return true;
            }

            return false;
        }

        public void Reset()
        {
            _inner.Reset();
        }

        public void Dispose()
        {
            _inner.Dispose();
        }

        private void Checkpoint()
        {
            _topology.Checkpoint(CheckpointState, _inner.Current);
        }
    }
}
