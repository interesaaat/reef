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

using Org.Apache.REEF.Network.Elastic.Config.OperatorParameters;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    public class CheckpointableModel<T> : ICheckpointableState where T : struct 
    {
        [Inject]
        public CheckpointableModel([Parameter(typeof(Checkpointing))] int level)
        {
            Level = (CheckpointLevel)level;
        }

        private T[] State { get; set; }

        public CheckpointLevel Level { get; protected set; }

        public void MakeCheckpointable(T[] model)
        {
            State = model;
        }

        void ICheckpointableState.MakeCheckpointable(object model)
        {
            MakeCheckpointable(model as T[]);
        }

        // Create a copy of the state
        internal ICheckpointState GetState()
        {
            return new CheckpointState<T[]>
            {
                Level = Level,
                
                State = (T[])State.Clone()
            };
        }

        ICheckpointState ICheckpointableState.Checkpoint()
        {
            return GetState();
        }
    }
}
