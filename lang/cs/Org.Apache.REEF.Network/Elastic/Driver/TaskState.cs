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

using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Definition of the the different states in which a task can be.
    /// </summary>
    public enum TaskState
    {
        Init = 1,

        Queued = 2,

        Submitted = 3,

        Recovering = 4,

        Running = 5,

        Failed = 6,

        Completed = 7
    }

    /// <summary>
    /// Utility class used to recognize particular task states.
    /// </summary>
    public static class TaskStateUtils
    {
        private static List<TaskState> recoverable = new List<TaskState>() { TaskState.Failed, TaskState.Queued };

        private static List<TaskState> notRunnable = new List<TaskState>() { TaskState.Failed, TaskState.Completed };

        /// <summary>
        /// Whether a task is recoverable or not.
        /// </summary>
        /// <param name="state">The current state of the task</param>
        /// <returns>True if the task is recoverable</returns>
        internal static bool IsRecoverable(TaskState state)
        {
            return recoverable.Contains(state);
        }

        /// <summary>
        /// Whether a task can be run or not.
        /// </summary>
        /// <param name="state">The current state of the task</param>
        /// <returns>True if the task can be run</returns>
        internal static bool IsRunnable(TaskState state)
        {
            return !notRunnable.Contains(state);
        }
    }
}
