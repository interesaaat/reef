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

using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Interface for checkpointing some task state
    /// Clients can implement this interface and inject it into context service and task function to save the current task state
    /// </summary>
    internal class CheckpointService
    {
        public readonly Dictionary<int, SortedDictionary<int, ICheckpointState>> _checkpoints;
        private readonly int _limit;

        public CheckpointService([Parameter(typeof(ElasticServiceConfigurationOptions.NumCheckpoints))] int num,
           )
        {
            _limit = num;
            _checkpoints = new Dictionary<int, SortedDictionary<int, ICheckpointState>>();
        }

        public ICheckpointState GetCheckpoint(int iteration = -1)
        {
            if (!HasCheckpoint(iteration))
            {
                throw new IllegalStateException("Asking for checkpoint not in the service");
            }

            iteration = iteration < 0 ? _last : iteration;

            return _checkpoints[iteration];
        }

        public void Checkpoint(ICheckpointState state)
        {   
            if(!_checkpoints.ContainsKey(state.OperatorId))
            {
                _checkpoints.Add(state.OperatorId, new SortedDictionary<int, ICheckpointState>());
            }

            var checkpoint = _checkpoints[state.OperatorId];

            if (HasCheckpoint(state.Iteration))
            {
                throw new IllegalStateException("Going to checkpoint an already checkpointed state");
            }

            checkpoint.Add(state.Iteration, state);

            CheckSize(checkpoint);
        }

        private bool HasCheckpoint(SortedDictionary<int, ICheckpointState> checkpoint, int iteration = -1)
        {
            if (iteration < 0)
            {
                if (_last < 0)
                {
                    return false;
                }
                return true;
            }

            return _checkpoints.ContainsKey(iteration);
        }

        private void CheckSize(SortedDictionary<int, ICheckpointState> checkpoint)
        {
            if (checkpoint.Keys.Count > _limit)
            {
                var first = checkpoint.Keys.First();
                checkpoint.Remove(first);
            }
        }
    }
}
