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

        private readonly SortedDictionary<float, FailureState> transitionMap = new SortedDictionary<float, FailureState>()
        {
            { 0.01F, FailureState.ContinueAndReconfigure },
            { 0.02F, FailureState.ContinueAndReschedule },
            { 0.03F, FailureState.StopAndReschedule }
        };

        public DefaultFailureStateMachine()
        {
            _numDependencise = 0;
            _currentFailures = 0;
            _currentState = FailureState.Continue;

            _statusLock = new object();
        }

        public void AddDependencies(int deps)
        {
            lock (_statusLock)
            {
                _numDependencise += deps;
                _currentFailures -= deps;

                if (_currentState != FailureState.Continue)
                {
                    _currentState = transitionMap.CeilingEntry(_currentFailures / _numDependencise).Value;
                }
            }
        }

        public void RemoveDependencies(int deps)
        {
            lock (_statusLock)
            {
                _numDependencise -= deps;
                _currentFailures += deps;

                if (_currentState != FailureState.StopAndReschedule)
                {
                    _currentState = transitionMap.FloorEntry(_currentFailures / _numDependencise).Value;
                }
            }
        }

        public void SetThreashold(FailureState level, float threshold)
        {
            if (level == FailureState.Continue)
            {
                throw new ArgumentException("Cannot change the threshould for Continue state");
            }

            if (threshold == 0.0F)
            {
                throw new ArgumentException("Threshold have to me greater than 0");
            }

            float accum = 0;
            float delta = 0;
            bool changed = false;
            var old = transitionMap;

            foreach (float key in transitionMap.Keys)
            {
                if (transitionMap[key] == level)
                {
                    lock (_statusLock)
                    {
                        transitionMap.Remove(key);
                        accum += threshold;
                        delta = accum - key;
                        transitionMap.Add(accum, level);
                        changed = true;
                    }
                }
                else
                {
                    if (changed)
                    {
                        lock (_statusLock)
                        {
                            if (key + delta > 1)
                            {
                                throw new IllegalStateException(string.Format(CultureInfo.InvariantCulture, "Threashold for state {0} bigger than 1", transitionMap[key]));
                            }

                            transitionMap.Add(key + delta, transitionMap[key]);
                            transitionMap.Remove(key);
                        }
                    }
                    else
                    {
                        accum += key;
                    }
                }
            }
        }

        public IFailureStateMachine Clone
        {
            get
            {
                var newMachine = new DefaultFailureStateMachine();
                foreach (float key in transitionMap.Keys)
                {
                    newMachine.SetThreashold(transitionMap[key], key);
                }

                return newMachine;
            }
        }
    }

    public static class SortedDictionaryExtensions
    {
        private static Tuple<int, int> GetPossibleIndices<TKey, TValue>(SortedDictionary<TKey, TValue> dictionary, TKey key, bool strictlyDifferent, out List<TKey> list)
        {
            list = dictionary.Keys.ToList();
            int index = list.BinarySearch(key, dictionary.Comparer);
            if (index >= 0)
            {
                // exists
                if (strictlyDifferent)
                {
                    return Tuple.Create(index - 1, index + 1);
                }
                else
                {
                    return Tuple.Create(index, index);
                }
            }
            else
            {
                // doesn't exist
                int indexOfBiggerNeighbour = ~index; // bitwise complement of the return value

                if (indexOfBiggerNeighbour == list.Count)
                {
                    // bigger than all elements
                    return Tuple.Create(list.Count - 1, list.Count);
                }
                else if (indexOfBiggerNeighbour == 0)
                {
                    // smaller than all elements
                    return Tuple.Create(-1, 0);
                }
                else
                {
                    // Between 2 elements
                    int indexOfSmallerNeighbour = indexOfBiggerNeighbour - 1;
                    return Tuple.Create(indexOfSmallerNeighbour, indexOfBiggerNeighbour);
                }
            }
        }

        public static KeyValuePair<TKey, TValue> FloorEntry<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            var indices = GetPossibleIndices(dictionary, key, false, out List<TKey> list);
            if (indices.Item1 < 0)
            {
                return default(KeyValuePair<TKey, TValue>);
            }

            var newKey = list[indices.Item1];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }

        public static KeyValuePair<TKey, TValue> CeilingEntry<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            var indices = GetPossibleIndices(dictionary, key, false, out List<TKey> list);
            if (indices.Item2 == list.Count)
            {
                return default(KeyValuePair<TKey, TValue>);
            }

            var newKey = list[indices.Item2];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }
    }
}
