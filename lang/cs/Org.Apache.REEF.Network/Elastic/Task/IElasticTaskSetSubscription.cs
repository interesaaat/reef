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

using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Annotations;
using System;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    ///  Used by Tasks to fetch Operators in the subscriptions configured by the driver.
    /// </summary>
    [DefaultImplementation(typeof(DefaultTaskSetSubscription))]
    public interface IElasticTaskSetSubscription : IWaitForTaskRegistration, IDisposable
    {
        /// <summary>
        /// Returns the subscription name
        /// </summary>
        string SubscriptionName { get; }

        /// <summary>
        /// Gets the Broadcast operator with the given id and message type.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="operatorName">The name of the Broadcast operator</param>
        /// <returns>The Broadcast operator</returns>
        IElasticBroadcast<T> GetBroadcast<T>(int operatorId);

        /// <summary>
        /// Gets the Iterate operator with the given id and type.
        /// </summary>
        /// <typeparam name="T">The iterator type</typeparam>
        /// <param name="operatorName">The name of the Iterate operator</param>
        /// <returns>The Iterate operator</returns>
        IElasticIterator<T> GetIterator<T>(int operatorId);
    }
}
