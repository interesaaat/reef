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
using System.Linq;

/// <summary>
/// The default implementation of the failure state machine.
/// This implementation works only with default failure states.
/// </summary>
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
            { DefaultFailureStates.ContinueAndReconfigure, 0.10F },
            { DefaultFailureStates.ContinueAndReschedule, 0.20F },
            { DefaultFailureStates.StopAndReschedule, 0.30F }
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
            get { return _currentState; }
        }

        public IFailureState AddDataPoints(int points)
        {
            lock (_statusLock)
            {
                if (_currentFailures == 0)
                {
                    _numDependencise += points;
                }
                else
                {
                    _currentFailures -= points;

                    if (_currentState.FailureState != (int)DefaultFailureStates.Continue)
                    {
                        float currentRate = _currentFailures / _numDependencise;

                        while (currentRate < transitionWeights[(DefaultFailureStates)_currentState.FailureState])
                        {
                            _currentState.FailureState = (int)transitionMapDown[(DefaultFailureStates)_currentState.FailureState];
                        }
                    }
                }

                return _currentState;
            }
        }

        public IFailureState RemoveDataPoints(int points)
        {
            lock (_statusLock)
            {
                _currentFailures += points;

                float currentRate = (float)_currentFailures / _numDependencise;

                while (_currentState.FailureState != (int)DefaultFailureStates.StopAndReschedule && currentRate > transitionWeights[transitionMapUp[(DefaultFailureStates)_currentState.FailureState]])
                {
                    _currentState.FailureState = (int)transitionMapUp[(DefaultFailureStates)_currentState.FailureState];
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

        public IFailureStateMachine Clone()
        {
            var newMachine = new DefaultFailureStateMachine();

            foreach (DefaultFailureStates state in transitionWeights.Keys)
            {
                newMachine.SetThreashold(new DefaultFailureState((int)state), transitionWeights[state]);
            }

            return newMachine;
        }

        public int NumOfDataPoints
        {
            get { return _numDependencise; }

            set { _numDependencise = value; }
        }

        public int NumOfFailedDataPoints
        {
            get { return _currentFailures; }

            set { _currentFailures = value; }
        }
    }
}
