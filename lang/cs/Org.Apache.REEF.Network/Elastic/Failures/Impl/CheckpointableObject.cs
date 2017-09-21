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

using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using Org.Apache.REEF.Tang.Annotations;
using System;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    // If the object is an enumerable the inner objects will not be cloned. Gotta fix this
    public class CheckpointableObject<T> : ICheckpointableState where T : ICloneable
    {
        [Inject]
        public CheckpointableObject()
        {
        }

        protected T State { get; set; }

        public CheckpointLevel Level { get; internal set; }

        public int Iteration { get; internal set; }

        public void MakeCheckpointable(T model)
        {
            State = model;
        }

        void ICheckpointableState.MakeCheckpointable(object model)
        {
            MakeCheckpointable((T)model);
        }

        // Create a copy of the state
        internal CheckpointState GetState()
        {
            switch (Level)
            {
                case CheckpointLevel.EphemeralMaster:
                case CheckpointLevel.EphemeralAll:
                    return new CheckpointState(Level, Iteration, State);
                case CheckpointLevel.PersistentMemoryMaster:
                case CheckpointLevel.PersistentMemoryAll:
                    return new CheckpointState(Level, Iteration, State.Clone());
                default:
                    return new CheckpointState();
            }
        }

        CheckpointState ICheckpointableState.Checkpoint()
        {
            return GetState();
        }
    }
}
