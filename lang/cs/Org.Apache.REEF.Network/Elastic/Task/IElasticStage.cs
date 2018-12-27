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

using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Annotations;
using System;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    ///  Used by tasks to fetch the workflow of the stages configured by the driver.
    /// </summary>
    [DefaultImplementation(typeof(DefaultTaskSetStage))]
    public interface IElasticStage : IWaitForTaskRegistration, IDisposable
    {
        /// <summary>
        /// The name of the Stage
        /// </summary>
        string StageName { get; }

        /// <summary>
        /// TODO
        /// </summary>
        void Cancel();

        /// <summary>
        /// The workflow of operators
        /// </summary>
        Workflow Workflow { get; }
    }
}
