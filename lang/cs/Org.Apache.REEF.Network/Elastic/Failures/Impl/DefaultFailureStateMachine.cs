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
        private bool _finalized;

        private readonly object _statusLock;

        private readonly SortedDictionary<DefaultFailureStates, DefaultFailureStates> transitionMapUp = new SortedDictionary<DefaultFailureStates, DefaultFailureStates>()
        {
            { DefaultFailureStates.Continue, DefaultFailureStates.ContinueAndReconfigure },
            { DefaultFailureStates.ContinueAndReconfigure, DefaultFailureStates.ContinueAndReschedule },
            { DefaultFailureStates.ContinueAndReschedule, DefaultFailureStates.StopAndReschedule },
            { DefaultFailureStates.StopAndReschedule, DefaultFailureStates.Fail }
        };

        private readonly SortedDictionary<DefaultFailureStates, DefaultFailureStates> transitionMapDown = new SortedDictionary<DefaultFailureStates, DefaultFailureStates>()
        {
            { DefaultFailureStates.ContinueAndReconfigure, DefaultFailureStates.Continue },
            { DefaultFailureStates.ContinueAndReschedule, DefaultFailureStates.ContinueAndReconfigure },
            { DefaultFailureStates.StopAndReschedule, DefaultFailureStates.ContinueAndReschedule }
        };

        private readonly IDictionary<DefaultFailureStates, float> transitionWeights = new Dictionary<DefaultFailureStates, float>()
        {
            { DefaultFailureStates.ContinueAndReconfigure, 0.0F },
            { DefaultFailureStates.ContinueAndReschedule, 0.5F },
            { DefaultFailureStates.StopAndReschedule, 0.5F },
            { DefaultFailureStates.Fail, 0.5F }
        };

        [Inject]
        public DefaultFailureStateMachine()
        {
            NumOfDataPoints = 0;
            NumOfFailedDataPoints = 0;
            State = new DefaultFailureState();
            _finalized = false;

            _statusLock = new object();
        }

        public IFailureState State { get; private set; }

        public int NumOfDataPoints { get; private set; }

        public int NumOfFailedDataPoints { get; private set; }

        public IFailureState AddDataPoints(int points)
        {
            lock (_statusLock)
            {
                if (!_finalized)
                {
                    NumOfDataPoints += points;
                }
                else
                {
                    NumOfFailedDataPoints -= points;

                    if (State.FailureState != (int)DefaultFailureStates.Continue)
                    {
                        float currentRate = NumOfFailedDataPoints / NumOfDataPoints;

                        while (currentRate < transitionWeights[(DefaultFailureStates)State.FailureState])
                        {
                            State.FailureState = (int)transitionMapDown[(DefaultFailureStates)State.FailureState];
                        }
                    }
                }

                return State;
            }
        }

        public IFailureState RemoveDataPoints(int points)
        {
            lock (_statusLock)
            {
                NumOfFailedDataPoints += points;

                float currentRate = (float)NumOfFailedDataPoints / NumOfDataPoints;

                while (State.FailureState != (int)DefaultFailureStates.Fail && 
                    currentRate > transitionWeights[transitionMapUp[(DefaultFailureStates)State.FailureState]])
                {
                    State.FailureState = (int)transitionMapUp[(DefaultFailureStates)State.FailureState];
                }

                return State;
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

        public void SetThreasholds(Tuple<IFailureState, float>[] weights)
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

        public IFailureStateMachine Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Failure state machine cannot be built more than once");
            }

            _finalized = true;

            return this;
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
    }
}
