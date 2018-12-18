﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Interface for checkpointing some task state.
    /// Clients can implement this interface and inject it into operators to save the current task state.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface ICheckpointableState
    {
        /// <summary>
        /// The current checkpoint level.
        /// </summary>
        CheckpointLevel Level { get; }

        /// <summary>
        /// Make the given input state a checkpointable state.
        /// </summary>
        /// <param name="state"></param>
        void MakeCheckpointable(object state);

        /// <summary>
        /// Checkpoint the current state.
        /// </summary>
        /// <returns></returns>
        ICheckpointState Checkpoint();

        /// <summary>
        /// Create a new empty checkpointable state from the current one.
        /// </summary>
        /// <param name="iteration">The current iteration for which we need to create a new checkpointable state</param>
        /// <returns></returns>
        ICheckpointableState From(int iteration = 0);
    }
}
