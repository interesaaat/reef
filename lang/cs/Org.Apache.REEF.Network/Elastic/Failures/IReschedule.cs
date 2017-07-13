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

using Org.Apache.REEF.Utilities;
using System.Collections.Generic;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Driver.Context;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Reschedule task event.
    /// </summary>
    public interface IReschedule : IFailureEvent
    {
        Optional<IActiveContext> ActiveContext { get; }

        IList<IFailedContext> FailedContexts { get; }

        Optional<IFailedTask> FailedTask { get; }
    }
}
