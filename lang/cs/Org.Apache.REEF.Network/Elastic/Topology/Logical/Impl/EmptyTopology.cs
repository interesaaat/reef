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
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Driver;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    class EmptyTopology : ITopology
    {
        private bool _finalized;

        public EmptyTopology()
        {
            _finalized = false;
            OperatorId = -1;
        }

        public int OperatorId { get; set; }

        public int AddTask(string taskId)
        {
            return 1;
        }

        public int RemoveTask(string taskId)
        {
            return 0;
        }

        public ITopology Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Topology cannot be built more than once");
            }

            if (OperatorId <= 0)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any operator");
            }

            _finalized = true;

            return this;
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
        }

        public string LogTopologyState()
        {
            return "empty";
        }

        public List<IElasticDriverMessage> Reconfigure(string taskId, string info)
        {
            return new List<IElasticDriverMessage>();
        }
    }
}
