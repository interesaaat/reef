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

using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Message used to communicated checkpoints between nodes in order to
    /// recover execution.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class CheckpointMessage : GroupCommunicationMessage
    {
        /// <summary>
        /// Constructor for a message containig a checkpoint.
        /// </summary>
        /// <param name="checkpoint">The checkpoint state</param>
        public CheckpointMessage(ICheckpointState checkpoint) : base(checkpoint.SubscriptionName, checkpoint.OperatorId)
        {
            Checkpoint = checkpoint;
        }

        /// <summary>
        /// The checkpoint contained in the message.
        /// </summary>
        public ICheckpointState Checkpoint { get; internal set; }

        /// <summary>
        /// Clone the message.
        /// </summary>
        public override object Clone()
        {
            return new CheckpointMessage(Checkpoint);
        }
    }
}