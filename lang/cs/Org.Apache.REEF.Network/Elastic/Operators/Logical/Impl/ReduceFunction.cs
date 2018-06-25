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

using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Tang.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical
{
    /// <summary>
    /// The class used to aggregate messages sent by ReduceSenders.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    public abstract class ReduceFunction<T>
    {
        /// <summary>
        /// Whether the reduce function is associative and commutative
        /// </summary>
        public abstract bool CanMerge { get; }

        /// <summary>
        /// Whether the reduce function requires messages to be sorted by task id
        /// </summary>
        public abstract bool RequireSorting { get; }

        /// <summary>
        /// Reduce the IEnumerable of messages into one message.
        /// Assume that this method destroys the input elements.
        /// </summary>
        /// <param name="elements">The messages to reduce</param>
        /// <returns>The reduced message</returns>
        internal void OnlineReduce(Queue<Tuple<string, GroupCommunicationMessage>> elements, DataMessage<T> next)
        {
            if (RequireSorting)
            {
                throw new IllegalStateException("In online reduce but sorting of the element is required");
            }

            if (elements.Count != 1)
            {
                throw new IllegalStateException(string.Format("Expect 1 element, got {0}", elements.Count));
            }

            if (elements.Count > 0)
            {
                var elem = elements.Dequeue();
                var dataElement = elem.Item2 as DataMessage<T>;

                if (next.Iteration != dataElement.Iteration)
                {
                    Console.WriteLine("{0} is different than {1}", next.Iteration, dataElement.Iteration);
                    throw new IllegalStateException("Aggregating not matching iterations");
                }

                next.Data = Combine(next.Data, dataElement.Data);
            }

            elements.Enqueue(Tuple.Create(string.Empty, next as GroupCommunicationMessage));
        }

        /// <summary>
        /// Reduce the IEnumerable of messages into one message.
        /// Assume that this method destroys the input elements.
        /// </summary>
        /// <param name="elements">The messages to reduce</param>
        /// <returns>The reduced message</returns>
        internal GroupCommunicationMessage Reduce(Queue<Tuple<string, GroupCommunicationMessage>> elements)
        {
            IEnumerator<DataMessage<T>> messages;
            DataMessage<T> ground = null;

            if (RequireSorting)
            {
                messages = elements
                    .OrderBy(x => x.Item1)
                    .Select(x => x.Item2 as DataMessage<T>)
                    .GetEnumerator();
            }
            else
            {
                messages = elements
                    .Select(x => x.Item2 as DataMessage<T>)
                    .GetEnumerator();
            }

            while (messages.MoveNext())
            {
                if (ground == null)
                {
                    ground = messages.Current;
                }
                else
                {
                    if (ground.Iteration != messages.Current.Iteration)
                    {
                        Console.WriteLine("{0} is different than {1}", ground.Iteration, messages.Current.Iteration);
                        throw new IllegalStateException("Aggregating not matching iterations");
                    }

                    ground.Data = Combine(ground.Data, messages.Current.Data);
                }
            }

            return ground;
        }

        protected abstract T Combine(T left, T right);
    }
}
