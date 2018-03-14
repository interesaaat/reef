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

using Org.Apache.REEF.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    public class TopologyUpdate
    {
        public TopologyUpdate(string node, List<string> children, string root)
        {
            Node = node;
            Children = children;
            Root = root;
        }

        public TopologyUpdate(string node, List<string> children) : this(node, children, string.Empty)
        {
        }

        public string Node { get; private set; }

        public List<string> Children { get; private set; }

        public string Root { get; private set; }

        // 1 int for the size of node
        // The size of node
        // 1 int for the number of children
        // 1 int for the length of each children
        // The size of the string of each child
        // 1 int + the size of root if not null
        public int Size
        {
            get
            {
                var nodeSize = sizeof(int) + Node.Length;
                var childrenSize = sizeof(int) + (Children.Count * sizeof(int)) + Children.Sum(x => x.Length);
                var rootSize = sizeof(int) + Root.Length;
                
                return nodeSize + childrenSize + rootSize;
            }
        }

        internal static void Serialize(byte[] buffer, ref int offset, List<TopologyUpdate> updates)
        {
            byte[] tmpBuffer;

            foreach (var value in updates)
            {
                Buffer.BlockCopy(BitConverter.GetBytes(value.Node.Length), 0, buffer, offset, sizeof(int));
                offset += sizeof(int);
                tmpBuffer = ByteUtilities.StringToByteArrays(value.Node);
                Buffer.BlockCopy(tmpBuffer, 0, buffer, offset, tmpBuffer.Length);
                offset += tmpBuffer.Length;

                Buffer.BlockCopy(BitConverter.GetBytes(value.Children.Count), 0, buffer, offset, sizeof(int));
                offset += sizeof(int);
                foreach (var child in value.Children)
                {
                    tmpBuffer = ByteUtilities.StringToByteArrays(child);
                    Buffer.BlockCopy(BitConverter.GetBytes(tmpBuffer.Length), 0, buffer, offset, sizeof(int));
                    offset += sizeof(int);
                    Buffer.BlockCopy(tmpBuffer, 0, buffer, offset, tmpBuffer.Length);
                    offset += tmpBuffer.Length;
                }

                var rootSize = value.Root == null ? 0 : value.Root.Length;
                Buffer.BlockCopy(BitConverter.GetBytes(rootSize), 0, buffer, offset, sizeof(int));
                offset += sizeof(int);

                if (rootSize != 0)
                {
                    tmpBuffer = ByteUtilities.StringToByteArrays(value.Root);
                    Buffer.BlockCopy(tmpBuffer, 0, buffer, offset, tmpBuffer.Length);
                    offset += tmpBuffer.Length;
                }
            }
        }

        internal static List<TopologyUpdate> Deserialize(byte[] data, int totLength, int start)
        {
            var result = new List<TopologyUpdate>();
            var num = 0;
            var length = 0;
            var offset = 0;
            string value;
            string node;
            List<string> tmp;

            while (offset < totLength)
            {
                length = BitConverter.ToInt32(data, start + offset);
                offset += sizeof(int);
                node = ByteUtilities.ByteArraysToString(data, start + offset, length);
                offset += length;

                num = BitConverter.ToInt32(data, start + offset);
                offset += sizeof(int);
                tmp = new List<string>();
                for (int i = 0; i < num; i++)
                {
                    length = BitConverter.ToInt32(data, start + offset);
                    offset += sizeof(int);
                    value = ByteUtilities.ByteArraysToString(data, start + offset, length);
                    offset += length;
                    tmp.Add(value);
                }

                length = BitConverter.ToInt32(data, start + offset);
                offset += sizeof(int);
                if (length > 0)
                {
                    value = ByteUtilities.ByteArraysToString(data, start + offset, length);
                    result.Add(new TopologyUpdate(node, tmp, value));
                }
                else
                {
                    result.Add(new TopologyUpdate(node, tmp));
                }
            }

            return result;
        }
    }
}
