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
        private DefaultFailureState _currentState;

        private object _statusLock;

        private readonly SortedDictionary<DefaultFailureStates, DefaultFailureStates> transitionMapUp = new SortedDictionary<DefaultFailureStates, DefaultFailureStates>()
        {
            { DefaultFailureStates.Continue, DefaultFailureStates.ContinueAndReconfigure },
            { DefaultFailureStates.ContinueAndReconfigure, DefaultFailureStates.ContinueAndReschedule },
            { DefaultFailureStates.ContinueAndReschedule, DefaultFailureStates.StopAndReschedule }
        };

        private readonly SortedDictionary<DefaultFailureStates, DefaultFailureStates> transitionMapDown = new SortedDictionary<DefaultFailureStates, DefaultFailureStates>()
        {
            { DefaultFailureStates.ContinueAndReconfigure, DefaultFailureStates.Continue },
            { DefaultFailureStates.ContinueAndReschedule, DefaultFailureStates.ContinueAndReconfigure },
            { DefaultFailureStates.StopAndReschedule, DefaultFailureStates.ContinueAndReschedule }
        };

        private readonly IDictionary<DefaultFailureStates, float> transitionWeights = new Dictionary<DefaultFailureStates, float>()
        {
            { DefaultFailureStates.ContinueAndReconfigure, 0.99F },
            { DefaultFailureStates.ContinueAndReschedule, 0.99F },
            { DefaultFailureStates.StopAndReschedule, 0.99F }
        };

        [Inject]
        public DefaultFailureStateMachine()
        {
            _numDependencise = 0;
            _currentFailures = 0;
            _currentState = new DefaultFailureState();

            _statusLock = new object();
        }

        public IFailureState State 
        {
            get
            {
                return _currentState;
            }
        }

        public IFailureState AddDataPoints(int points)
        {
            lock (_statusLock)
            {
                _numDependencise += points;
                _currentFailures -= points;

                if (_currentState.FailureState != (int)DefaultFailureStates.Continue)
                {
                    while (_currentFailures / _numDependencise < transitionWeights[(DefaultFailureStates)_currentState.FailureState])
                    {
                        _currentState.FailureState = (int)transitionMapDown[(DefaultFailureStates)_currentState.FailureState];
                    }
                }

                return _currentState;
            }
        }

        public IFailureState RemoveDataPoints(int points)
        {
            lock (_statusLock)
            {
                var current = _currentState;
                _numDependencise -= points;
                _currentFailures += points;

                if (_currentState.FailureState != (int)DefaultFailureStates.StopAndReschedule)
                {
                    while (_currentFailures / _numDependencise > transitionWeights[transitionMapUp[(DefaultFailureStates)_currentState.FailureState]])
                    {
                        _currentState.FailureState = (int)transitionMapUp[(DefaultFailureStates)_currentState.FailureState];
                    }
                }

                return _currentState;
            }
        }

        public void SetThreashold(IFailureState level, float threshold)
        {
            if (!(level is DefaultFailureState))
            {
                throw new ArgumentException(level.GetType() + " is not DefaultFailureStateMachine");
            }

            if (level.FailureState == (int)DefaultFailureStates.Continue)
            {
                throw new ArgumentException("Cannot change the threshould for Continue state");
            }

            transitionWeights[(DefaultFailureStates)level.FailureState] = threshold;

            CheckConsistency();
        }

        public void SetThreasholds(params Tuple<IFailureState, float>[] weights)
        {
            if (!weights.All(weight => weight.Item1 is DefaultFailureState))
            {
                throw new ArgumentException("Input is not of type DefaultFailureStateMachine");
            }

            if (weights.Any(weight => weight.Item1.FailureState == (int)DefaultFailureStates.Continue))
            {
                throw new ArgumentException("Cannot change the threshould for Continue state");
            }

            foreach (Tuple<IFailureState, float> weight in weights)
            {
                transitionWeights[(DefaultFailureStates)weight.Item1.FailureState] = weight.Item2;
            }

            CheckConsistency();
        }

        private void CheckConsistency()
        {
            var state = DefaultFailureStates.ContinueAndReconfigure;
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
                if (state == DefaultFailureStates.StopAndReschedule)
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

                foreach (DefaultFailureStates state in transitionWeights.Keys)
                {
                    newMachine.SetThreashold(new DefaultFailureState((int)state), transitionWeights[state]);
                }

                return newMachine;
            }
        }
    }
}
