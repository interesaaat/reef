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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Failures.Enum
{
    /// <summary>
    /// Definition of supported checkpointing policies.
    /// </summary>
    [Unstable("0.16", "Policies may change")]
    public enum CheckpointLevel : int
    {
        None = 0, // No checkpointing

        EphemeralMaster = 10, // Checkpointing on the master task, not tolerant to task failures

        EphemeralAll = 11, // Checkpointing on all tasks, not tolerant to task failures

        PersistentMemoryMaster = 20, // Checkpointing on the master memory, tolerant to task failures but not evaluator failures

        PersistentMemoryAll = 21 // Checkpointing on the tasks memory, tolerant to tasks failures but not evaluators failures
    }
}
