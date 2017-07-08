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
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public class FailureStateMachine : IFailureStateMachine
    {
        private int _numDependencise;
        private int _currentFailures;
        private FailureState _currentState;

        private object _statusLock;

        private readonly SortedDictionary<float, FailureState> transitionMap = new SortedDictionary<float, FailureState>()
        {
            {0.01F, FailureState.ContinueAndReconfigure },
            {0.02F, FailureState.ContinueAndReschedule },
            {0.03F, FailureState.StopAndReschedule }
        };

        public FailureStateMachine(int numDeps)
        {
            _numDependencise = numDeps;
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
                    _currentState = transitionMap[transitionMap.FloorKey((float)_currentFailures / _numDependencise)];
                }
            }
        }

        private FailureState GetNext(FailureStateEvent taskEvent)
        {
            StateTransition<FailureState, FailureStateEvent> transition = new StateTransition<FailureState, FailureStateEvent>(_currentState, taskEvent);
            FailureState nextState;
            if (!Transitions.TryGetValue(transition, out nextState))
            {
                throw new IllegalStateException(string.Format(CultureInfo.InvariantCulture, "Unexpected event {0} in state {1}.", _currentState, taskEvent));
            }
            return nextState;
        }

        /// <summary>
        /// Move to the next state
        /// If it is not able to move to the next valid state for a given event, TaskStateTransitionException will be thrown.
        /// </summary>
        /// <param name="taskEvent"></param>
        /// <returns></returns>
        private FailureState MoveUp(FailureStateEvent taskEvent)
        {
            _currentState = GetNext(taskEvent);
            return _currentState;
        }

        public void RemoveDependencies(int deps)
        {
            Interlocked.Add(ref _numDependencise, -deps);
        }

        public void SetThreashold(FailureState level1, FailureState level2, float threshold);
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
                    return Tuple.Create(index - 1, index + 1);
                else
                    return Tuple.Create(index, index);
            }
            else
            {
                // doesn't exist
                int indexOfBiggerNeighbour = ~index; //bitwise complement of the return value

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

        public static TKey LowerKey<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, true, out list);
            if (indices.Item1 < 0)
                return default(TKey);

            return list[indices.Item1];
        }
        public static KeyValuePair<TKey, TValue> LowerEntry<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, true, out list);
            if (indices.Item1 < 0)
                return default(KeyValuePair<TKey, TValue>);

            var newKey = list[indices.Item1];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }

        public static TKey FloorKey<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, false, out list);
            if (indices.Item1 < 0)
                return default(TKey);

            return list[indices.Item1];
        }
        public static KeyValuePair<TKey, TValue> FloorEntry<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, false, out list);
            if (indices.Item1 < 0)
                return default(KeyValuePair<TKey, TValue>);

            var newKey = list[indices.Item1];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }

        public static TKey CeilingKey<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, false, out list);
            if (indices.Item2 == list.Count)
                return default(TKey);

            return list[indices.Item2];
        }
        public static KeyValuePair<TKey, TValue> CeilingEntry<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, false, out list);
            if (indices.Item2 == list.Count)
                return default(KeyValuePair<TKey, TValue>);

            var newKey = list[indices.Item2];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }

        public static TKey HigherKey<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, true, out list);
            if (indices.Item2 == list.Count)
                return default(TKey);

            return list[indices.Item2];
        }
        public static KeyValuePair<TKey, TValue> HigherEntry<TKey, TValue>(this SortedDictionary<TKey, TValue> dictionary, TKey key)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, true, out list);
            if (indices.Item2 == list.Count)
                return default(KeyValuePair<TKey, TValue>);

            var newKey = list[indices.Item2];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }
    }
}
