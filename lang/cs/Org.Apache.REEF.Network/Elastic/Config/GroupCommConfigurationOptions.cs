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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Elastic.Config
{
    public sealed class GroupCommConfigurationOptions
    {
        [NamedParameter("Driver identifier")]
        public class DriverId : Name<string>
        {
        }

        [NamedParameter("Serialized communication group configuration")]
        public class SerializedGroupConfigs : Name<ISet<string>>
        {
        }

        [NamedParameter("Id of root task in operator topology")]
        public class TopologyRootTaskId : Name<int>
        {
        }

        [NamedParameter("Ids of child tasks in operator topology")]
        public class TopologyChildTaskIds : Name<ISet<int>>
        {
        }
    }
}
