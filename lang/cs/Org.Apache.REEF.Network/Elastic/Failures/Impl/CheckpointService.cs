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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
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
        private static readonly Logger Logger = Logger.GetLogger(typeof(CheckpointService));

        public readonly Dictionary<int, SortedDictionary<int, ICheckpointState>> _checkpoints;
        private readonly int _limit;

        [Inject]
        public CheckpointService(
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumCheckpoints))] int num)
        {
            _limit = num;
            _checkpoints = new Dictionary<int, SortedDictionary<int, ICheckpointState>>();
        }

        public ICheckpointState GetCheckpoint(int operatorId, int iteration = -1)
        {
            if (!_checkpoints.ContainsKey(operatorId))
            {
                Logger.Log(Level.Warning, "Asking for checkpoint not in the service");
            }

            var checkpoints = _checkpoints[operatorId];

            iteration = iteration < 0 ? _checkpoints.Keys.Last() : iteration;

            return checkpoints[iteration];
        }

        public void Checkpoint(ICheckpointState state)
        {   
            if (!_checkpoints.ContainsKey(state.OperatorId))
            {
                _checkpoints.Add(state.OperatorId, new SortedDictionary<int, ICheckpointState>());
            }

            var checkpoint = _checkpoints[state.OperatorId];

            checkpoint.Add(state.Iteration, state);

            CheckSize(checkpoint);
        }
        
        public void RemoveCheckpoint(int operatorId)
        {
            if (_checkpoints.ContainsKey(operatorId))
            {
                _checkpoints.Remove(operatorId);
            }
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
