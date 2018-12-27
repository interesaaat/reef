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

using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Checkpointable state wrapping a mutable object. 
    /// Since the state is mutable, when we create a checkpoint we need to clone the state.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Unstable("0.16", "API may change")]
    public sealed class CheckpointableMutableObject<T> : CheckpointableImmutableObject<T> where T : ICloneable
    {
        [Inject]
        private CheckpointableMutableObject(
            [Parameter(typeof(OperatorParameters.Checkpointing))] int level,
            ICheckpointState checkpoint) : base(level, checkpoint)
        {
        }

        /// <summary>
        /// Checkpoint the current state.
        /// </summary>
        /// <returns>A checkpoint state</returns>
        public override ICheckpointState Checkpoint()
        {
            switch (Level)
            {
                case CheckpointLevel.EphemeralMaster:
                case CheckpointLevel.EphemeralAll:
                case CheckpointLevel.PersistentMemoryMaster:
                case CheckpointLevel.PersistentMemoryAll:
                    return _checkpoint.Create(State.Clone());
                default:
                    throw new ArgumentException($"Level {Level} not recognized.");
            }
        }

        /// <summary>
        /// Create a new empty checkpointable state from the current one.
        /// </summary>
        /// <param name="iteration">The current iteration for which we need to create a new checkpointable state</param>
        /// <returns>An empty checkpointable state</returns>
        public override ICheckpointableState Create()
        {
            return new CheckpointableMutableObject<T>((int)Level, _checkpoint);
        }
    }
}
