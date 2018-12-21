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

using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Class wrapping a state that has been checkpointed or is ready to.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public sealed class DefaultCheckpointState : ICheckpointState
    {
        [Inject]
        private DefaultCheckpointState()
        {
            Iteration = -1;
            OperatorId = -1;
            SubscriptionName = string.Empty;
        }

        /// <summary>
        /// The iteration number for this checkpoint.
        /// </summary>
        public int Iteration { get; set; }

        /// <summary>
        /// The operator id for this checkpoint.
        /// </summary>
        public int OperatorId { get; set; }

        /// <summary>
        /// The subscription name of the checkpoint.
        /// </summary>
        public string SubscriptionName { get; set; }

        /// <summary>
        /// The actual state of the checkpoint.
        /// </summary>
        public object State { get; private set; }

        /// <summary>
        /// Create a new empty checkpoint from the settings of the current one.
        /// </summary>
        /// <returns>A checkpoint with no state but with properly set up fields</returns>
        public ICheckpointState Create(object state)
        {
            return new DefaultCheckpointState()
            {
                State = state,
            };
        }

        /// <summary>
        /// Utility method used to create message out of
        /// the checkpoint. This is used when checkpoints need
        /// to be sent among nodes to recover computation.
        /// </summary>
        /// <returns>A checkpoint ready to be communicated</returns>
        public GroupCommunicationMessage ToMessage()
        {
            return new CheckpointMessage(this);
        }
    }
}
