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

using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    public class Workflow : IEnumerator<IElasticOperator>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(Workflow));

        private int _position = -1;
        private bool _failed;
        private bool _disposed;
        private readonly object _lock;

        private readonly IList<IElasticOperator> _operators;
        private List<int> _iteratorsPosition;

        [Inject]
        private Workflow()
        {
            _operators = new List<IElasticOperator>();
            _failed = false;
            _disposed = false;
            _lock = new object();
            _iteratorsPosition = new List<int>();
        }

        public object Iteration
        {
            get
            {
                if (_iteratorsPosition.Count == 0)
                {
                    return 0;
                }
                else
                {
                    var iterPos = _iteratorsPosition[0];
                    var iterator = _operators[iterPos] as IElasticIterator;
                    return iterator.Current;
                }
            }
        }

        internal CancellationSource CancellationSource { get; set; }

        public void Add(IElasticOperator op)
        {
            op.CancellationSource = CancellationSource.Source;

            _operators.Add(op);

            if (_iteratorsPosition.Count > 0)
            {
                var iterPos = _iteratorsPosition.Last();
                var iterator = _operators[iterPos] as IElasticIterator;

                op.IteratorReference = iterator;
                iterator.RegisterActionOnTaskRescheduled(op.OnTaskRescheduled);
            }

            if (op.OperatorName == Constants.Iterate)
            {
                _iteratorsPosition.Add(_operators.Count - 1);
            }
        }

        public bool MoveNext()
        {
            _position++;

            if (_failed || CancellationSource.IsCancelled())
            {
                return false;
            }

            // Check if we need to iterate
            if (_iteratorsPosition.Count > 0 && _position == _iteratorsPosition[0])
            {
                var iteratorOperator = _operators[_position] as IElasticIterator;

                if (iteratorOperator.MoveNext())
                {
                    _position++;

                    ResetOperatorPositions();

                    return true;
                }
                else
                {
                    if (_iteratorsPosition.Count > 1)
                    {
                        _iteratorsPosition.RemoveAt(0);

                        _position = _iteratorsPosition[0] - 1;
                    }
                    
                    return false;
                }
            }

            // In case we have one or zero iterators (or we are at the last iterator when multiple iterators exists)
            if (_position >= _operators.Count || (_iteratorsPosition.Count > 1 && _position == _iteratorsPosition[1]))
            {
                if (_iteratorsPosition.Count == 0)
                {
                    return false;
                }
                else
                {
                    _position = _iteratorsPosition[0] - 1;

                    return MoveNext();
                }
            }

            return true;
        }

        public void Throw(Exception e)
        {
            if (CancellationSource.IsCancelled())
            {
                Logger.Log(Level.Warning, "Workflow captured an Exception while Cancellation Source is True", e);
            }
            else
            {
                Logger.Log(Level.Error, "Workflow captured an Exception", e);
                _failed = true;

                throw new OperatorException(
                    "Workflow captured an Exception", Current.OperatorId, e, Current.FailureInfo);
            }     
        }

        public void Reset()
        {
            if (_iteratorsPosition.Count > 0)
            {
                _position = _iteratorsPosition[0];
            }
            else
            {
                _position = 0;
            }
        }

        public IElasticOperator Current
        {
            get
            {
                // If the workflow is composed by an iterator operator only, return a empty operator
                if (_operators.Count == 1 && _iteratorsPosition.Count > 0 &&_iteratorsPosition[0] > 0)
                {
                    return new EmptyOperator();
                }

                return _position == -1 ? _operators[0] : _operators[_position];
            }
        }

        public ICheckpointableState GetCheckpointableState()
        {
            if (_iteratorsPosition.Count > 0 && _operators[_iteratorsPosition[0]] is ICheckpointingOperator)
            {
                var checkpointable = (_operators[_iteratorsPosition[0]] as ICheckpointingOperator).CheckpointState;

                 return checkpointable;
            }

            throw new IllegalStateException("No checkpointable state enabled for this workflow");
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (!_disposed)
                {
                    if (_operators != null)
                    {
                        // Clean dispose, check that the computation is completed
                        if (_failed == false)
                        {
                            foreach (var op in _operators)
                            {
                                if (op != null)
                                {
                                    op.WaitCompletionBeforeDisposing();
                                }
                            }
                        }

                        foreach (var op in _operators)
                        {
                            if (op != null)
                            {
                                var disposableOperator = op as IDisposable;

                                disposableOperator.Dispose();
                            }
                        }
                    }

                    _disposed = true;
                }
            }
        }

        internal void WaitForTaskRegistration(CancellationTokenSource cancellationSource = null)
        {
            try
            {
                foreach (var op in _operators)
                {
                    op.WaitForTaskRegistration(cancellationSource);
                }
            }
            catch (OperationCanceledException e)
            {
                throw e;
            }
        }

        private void ResetOperatorPositions()
        {
            for (int pos = _position; pos < _operators.Count; pos++)
            {
                _operators[pos].ResetPosition();
            }
        }
    }
}
