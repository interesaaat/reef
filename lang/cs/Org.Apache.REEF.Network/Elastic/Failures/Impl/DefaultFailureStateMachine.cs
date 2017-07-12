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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    public class DefaultFailureStateMachine : IFailureStateMachine
    {
        private int _numDependencise;
        private int _currentFailures;
        private FailureState _currentState;

        private object _statusLock;

        private readonly SortedDictionary<FailureState, FailureState> transitionMapUp = new SortedDictionary<FailureState, FailureState>()
        {
            { FailureState.Continue, FailureState.ContinueAndReconfigure },
            { FailureState.ContinueAndReconfigure, FailureState.ContinueAndReschedule },
            { FailureState.ContinueAndReschedule, FailureState.StopAndReschedule }
        };

        private readonly SortedDictionary<FailureState, FailureState> transitionMapDown = new SortedDictionary<FailureState, FailureState>()
        {
            { FailureState.ContinueAndReconfigure, FailureState.Continue },
            { FailureState.ContinueAndReschedule, FailureState.ContinueAndReconfigure },
            { FailureState.StopAndReschedule, FailureState.ContinueAndReschedule }
        };

        private readonly SortedDictionary<FailureState, float> transitionWeights = new SortedDictionary<FailureState, float>()
        {
            { FailureState.ContinueAndReconfigure, 0.99F },
            { FailureState.ContinueAndReschedule, 0.99F },
            { FailureState.StopAndReschedule, 0.99F }
        };

        [Inject]
        public DefaultFailureStateMachine()
        {
            _numDependencise = 0;
            _currentFailures = 0;
            _currentState = FailureState.Continue;

            _statusLock = new object();
        }

        public FailureState State 
        {
            get
            {
                return _currentState;
            }
        }

        public FailureState AddDataPoints(int points)
        {
            lock (_statusLock)
            {
                _numDependencise += points;
                _currentFailures -= points;

                if (_currentState != FailureState.Continue)
                {
                    while (_currentFailures / _numDependencise < transitionWeights[_currentState])
                    {
                        _currentState = transitionMapDown[_currentState];
                    }
                }

                return _currentState;
            }
        }

        public FailureState RemoveDataPoints(int points)
        {
            lock (_statusLock)
            {
                var current = _currentState;
                _numDependencise -= points;
                _currentFailures += points;

                if (_currentState != FailureState.StopAndReschedule)
                {
                    while (_currentFailures / _numDependencise > transitionWeights[transitionMapUp[_currentState]])
                    {
                        _currentState = transitionMapUp[_currentState];
                    }
                }

                return _currentState;
            }
        }

        public void SetThreashold(FailureState level, float threshold)
        {
            if (level == FailureState.Continue)
            {
                throw new ArgumentException("Cannot change the threshould for Continue state");
            }

            transitionWeights[level] = threshold;

            CheckConsistency();
        }

        public void SetThreasholds(params Tuple<FailureState, float>[] weights)
        {
            if (weights.Any(weight => weight.Item1 == FailureState.Continue))
            {
                throw new ArgumentException("Cannot change the threshould for Continue state");
            }

            foreach (Tuple<FailureState, float> weight in weights)
            {
                transitionWeights[weight.Item1] = weight.Item2;
            }

            CheckConsistency();
        }

        private void CheckConsistency()
        {
            var state = FailureState.ContinueAndReconfigure;
            float prevWeight = transitionWeights[state];
            state = transitionMapUp[state];
            float nextWeight = transitionWeights[state];

            while (nextWeight >= 0)
            {
                if (nextWeight < prevWeight)
                {
                    throw new IllegalStateException("State " + transitionMapDown[state] + " weight is bigger than state " + state);
                }

                prevWeight = nextWeight;
                if (state == FailureState.StopAndReschedule)
                {
                    return;
                }
                state = transitionMapUp[state];
                transitionWeights.TryGetValue(state, out nextWeight);
            }
        }

        public IFailureStateMachine Clone
        {
            get
            {
                var newMachine = new DefaultFailureStateMachine();
                foreach (FailureState state in transitionWeights.Keys)
                {
                    newMachine.SetThreashold(state, transitionWeights[state]);
                }

                return newMachine;
            }
        }
    }
}
