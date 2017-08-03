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

using Org.Apache.REEF.Tang.Annotations;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Config
{
    public class OperatorsConfiguration
    {
        [NamedParameter("Operator Name")]
        public class OperatorType : Name<string>
        {
        }

        [NamedParameter("Type of the message")]
        public class MessageType : Name<string>
        {
        }

        [NamedParameter("Operator Id")]
        public class OperatorId : Name<int>
        {
        }

        [NamedParameter("Number of iterations")]
        public class NumIterations : Name<int>
        {
        }
    }
}
