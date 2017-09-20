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
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Used to propagate task messages through operators and subscriptions.
    /// </summary>
    public interface ITaskMessageResponse
    {
        /// <summary>
        /// Method triggered when a Task to Driver message is received. 
        /// </summary>
        /// <param name="message">The task message for the operator</param>
        /// <param name="returnMessages">A list of messages containing the instructions for the task</param>
        /// <returns>Zero or more messages for the tasks</returns>
        void OnTaskMessage(ITaskMessage message, ref List<IDriverMessage> returnMessages);
    }
}