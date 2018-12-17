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

using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    public class ForLoopEnumerator : ElasticIteratorEnumerator<int>
    {
        private int _iterations;
        private readonly int _start;

        [Inject]
        private ForLoopEnumerator(
            [Parameter(typeof(OperatorParameters.NumIterations))] int iterations,
            [Parameter(typeof(OperatorParameters.StartIteration))] int start)
        {
            _iterations = iterations;
            _start = start - 1;
            State = _start;
        }

        public override bool MoveNext()
        {
            var result = State < _iterations;

            if (result)
            {
                State++;
            }

            return result;
        }

        public override int Current
        {
            get { return State; }
        }

        public override bool IsStart
        {
            get { return Current == _start; }
        }
    }
}
