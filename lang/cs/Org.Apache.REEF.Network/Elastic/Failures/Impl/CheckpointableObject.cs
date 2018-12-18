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
    // Is users duty to make sure that if T is an IEnumerable type, the inner objects are clonable as well.
    [Unstable("0.16", "API may change")]
    public class CheckpointableImmutableObject<T> : ICheckpointableState
    {
        [Inject]
        private CheckpointableImmutableObject([Parameter(typeof(OperatorParameters.Checkpointing))] int level) : base()
        {
            Level = (CheckpointLevel)level;
        }

        private CheckpointableImmutableObject()
        {
            State = default;
            Iteration = 0;
        }

        protected T State { get; set; }

        public CheckpointLevel Level { get; internal set; }

        public int Iteration { get; internal set; }

        public void MakeCheckpointable(object model)
        {
            State = (T)model;
        }

        // Create a copy of the state
        public ICheckpointState Checkpoint()
        {
            switch (Level)
            {
                case CheckpointLevel.EphemeralMaster:
                case CheckpointLevel.EphemeralAll:
                    return new CheckpointState<T>(Level, Iteration, State);
                case CheckpointLevel.PersistentMemoryMaster:
                case CheckpointLevel.PersistentMemoryAll:
                    return new CheckpointState<T>(Level, Iteration, State);
                default:
                    return new CheckpointState<T>();
            }
        }

        public ICheckpointableState From(int iteration = 0)
        {
            return new CheckpointableImmutableObject<T>()
            {
                Level = Level,
                Iteration = iteration
            };
        }
    }
}
