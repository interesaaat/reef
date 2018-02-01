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
using System.Collections.Generic;

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
        /// Reduce the IEnumerable of messages into one message.
        /// Assume that this method destroys the input elements.
        /// </summary>
        /// <param name="elements">The messages to reduce</param>
        /// <returns>The reduced message</returns>
        public GroupCommunicationMessage Reduce(IEnumerable<GroupCommunicationMessage> elements)
        {
            DataMessage<T> ground = null;

            foreach (var elem in elements)
            {
                var dataElement = elem as DataMessage<T>;
                if (ground == null)
                {
                    ground = dataElement;
                }
                else
                {
                    if (ground.Iteration != dataElement.Iteration)
                    {
                        throw new IllegalStateException("Aggregating not matching iterations");
                    }

                    ground.Data = Reduce(ground.Data, dataElement.Data);
                }
            }

            return ground;
        }

        protected abstract T Reduce(T left, T right);
    }
}
