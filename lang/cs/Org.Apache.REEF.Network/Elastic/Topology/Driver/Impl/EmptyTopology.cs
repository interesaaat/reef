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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using System;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Topology.Impl
{
    class EmptyTopology : ITopology
    {
        private bool _finalized;

        public EmptyTopology()
        {
            _finalized = false;
        }

        public int AddTask(string taskId)
        {
            return 1;
        }

        public int RemoveTask(string taskId)
        {
            return 0;
        }

        public void Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Topology cannot be built more than once");
            }

            _finalized = true;
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
        }

        public string LogTopologyState()
        {
            return "empty";
        }
    }
}
