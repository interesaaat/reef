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

using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Interface for checkpointing some task state
    /// Clients can implement this interface and inject it into context service and task function to save the current task state
    /// </summary>
    [DefaultImplementation(typeof(NoCheckpointableState))]
    public interface ICheckpointableState
    {
        CheckpointLevel Level { get; }

        void MakeCheckpointable(object state);

        ICheckpointState Checkpoint();

        IStreamingCodec<object> Codec { get; }
    }
}
