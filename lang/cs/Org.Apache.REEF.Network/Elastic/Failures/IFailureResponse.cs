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

using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Driver;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Entry point for classes expected to be aware and act over failures.
    /// Used to propagate failures through operators, subscriptions and the service.
    /// </summary>
    public interface IFailureResponse
    {
        /// <summary>
        /// Used to react on a failure occurred on a task.
        /// It gets a failed task as input and in response it produces zero or more failure events
        /// </summary>
        /// <param name="task">The failed task</param>
        /// <param name="failureEvents">A list of events encoding the type of action to be triggered</param>
        /// <returns>Zero or more events for triggering failure mitigation mechanisms</returns>
        void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents);

        // TODO
        void OnResume(ref List<IElasticDriverMessage> msgs, ref string taskId, ref int? iteration);

        /// <summary>
        /// When a new failure state is raised, this method is used to dispatch
        /// such event to the proper failure mitigation logic.
        /// It gets a failure event as input and produces zero or more failure response messages for tasks
        /// </summary>
        /// <param name="event">The failure event to react upon</param>
        /// <returns>Zero or more events for the tasks</returns>
        void EventDispatcher(ref IFailureEvent @event);
    }
}
