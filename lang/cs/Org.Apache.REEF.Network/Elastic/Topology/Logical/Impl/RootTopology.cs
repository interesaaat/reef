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

using System;
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Globalization;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Comm;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    public class RootTopology : ITopology
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(RootTopology));

        private int _rootId;
        private bool _finalized;
        private bool _hasRoot;

        public RootTopology(int rootId)
        {
            _rootId = rootId;
            _finalized = false;
            _hasRoot = false;
            OperatorId = -1;
        }

        public int OperatorId { get; set; }

        public string SubscriptionName { get; set; }

        public int AddTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            var id = Utils.GetTaskNum(taskId);

            if (id == _rootId)
            {
                if (_hasRoot)
                {
                    throw new IllegalStateException("Trying to add root node twice");
                }

                _hasRoot = true;
            }

            return 1;
        }

        public int RemoveTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }

            return 1;
        }

        public ITopology Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Topology cannot be built more than once");
            }

            if (!_hasRoot)
            {
                throw new IllegalStateException("Topology cannot be built because the root node is missing");
            }

            if (OperatorId <= 0)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any operator");
            }

            if (SubscriptionName == string.Empty)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any subscription");
            }

            _finalized = true;

            return this;
        }

        public string LogTopologyState()
        {
            return _rootId.ToString();
        }

        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
            confBuilder.BindNamedParameter<GroupCommunicationConfigurationOptions.TopologyRootTaskId, int>(
                    GenericType<GroupCommunicationConfigurationOptions.TopologyRootTaskId>.Class,
                    _rootId.ToString(CultureInfo.InvariantCulture));
        }

        public IList<IElasticDriverMessage> Reconfigure(string taskId, string info)
        {
            throw new MissingMethodException("TODO");
        }
    }
}
