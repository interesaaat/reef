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

using Org.Apache.REEF.Network.Elastic.Operators.Logical;
using Org.Apache.REEF.Tang.Annotations;
using System;

namespace Org.Apache.REEF.Network.Elastic.Operators
{
    public class IntSumFunction : ReduceFunction<int>
    {
        [Inject]
        public IntSumFunction()
        {
        }

        public override bool CanMerge
        {
            get { return false; }
        }

        public override bool RequireSorting
        {
            get { return false; }
        }

        protected override int Combine(int left, int right)
        {
            return left + right;
        }
    }

    public class UnionFunction<T> : ReduceFunction<T[]>
    {
        [Inject]
        public UnionFunction()
        {
        }

        public override bool CanMerge
        {
            get { return false; }
        }

        public override bool RequireSorting
        {
            get { return true; }
        }

        protected override T[] Combine(T[] left, T[] right)
        {
            var ll = left.Length;
            var rl = right.Length;

            Array.Resize(ref left, ll + rl);
            
            for (int i = 0; i < right.Length; i++)
            {
                left[ll + i] = right[i];
            }

            return left;
        }
    }
}
