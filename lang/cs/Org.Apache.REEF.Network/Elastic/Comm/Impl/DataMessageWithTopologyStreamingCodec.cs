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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Streaming Codec for the Group Communication Message
    /// </summary>
    internal sealed class DataMessageWithTopologyStreamingCodec<T> : IStreamingCodec<DataMessageWithTopology<T>>
    {
        private readonly IStreamingCodec<T> _codec;

        /// <summary>
        /// Empty constructor to allow instantiation by reflection
        /// </summary>
        [Inject]
        private DataMessageWithTopologyStreamingCodec(IStreamingCodec<T> codec)
        {
            _codec = codec;
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <returns>The Group Communication Message</returns>
        public DataMessageWithTopology<T> Read(IDataReader reader)
        {
            int metadataSize = reader.ReadInt32() + sizeof(int) + sizeof(int);
            byte[] metadata = new byte[metadataSize];
            reader.Read(ref metadata, 0, metadataSize);
            var res = MetaDataDecoding(metadata);

            string subscriptionName = res.Item1;
            int operatorId = res.Item2;
            int iteration = res.Item3;
            var data = _codec.Read(reader);

            return new DataMessageWithTopology<T>(subscriptionName, operatorId, iteration, data);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(DataMessageWithTopology<T> obj, IDataWriter writer)
        {
            byte[] encodedMetadata = MetaDataEncoding(obj);
   
            writer.Write(encodedMetadata, 0, encodedMetadata.Length);

            _codec.Write(obj.Data, writer);
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        /// <returns>The Group Communication Message</returns>
        public async Task<DataMessageWithTopology<T>> ReadAsync(IDataReader reader,
            CancellationToken token)
        {
            int metadataSize = await reader.ReadInt32Async(token);
            byte[] metadata = new byte[metadataSize];
            await reader.ReadAsync(metadata, 0, metadataSize, token);
            var res = MetaDataDecoding(metadata);
            
            var data = await _codec.ReadAsync(reader, token);
            string subscriptionString = res.Item1;
            int operatorId = res.Item2;
            int iteration = res.Item3;
            List<TopologyUpdate> update = res.Item4;

            return new DataMessageWithTopology<T>(subscriptionString, operatorId, iteration, data, update);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async System.Threading.Tasks.Task WriteAsync(DataMessageWithTopology<T> obj, IDataWriter writer, CancellationToken token)
        {
            byte[] encodedMetadata = MetaDataEncoding(obj);

            await writer.WriteAsync(BitConverter.GetBytes(encodedMetadata.Length), 0, sizeof(int), token);
            await writer.WriteAsync(encodedMetadata, 0, encodedMetadata.Length, token);

            await _codec.WriteAsync(obj.Data, writer, token);
        }

        private static byte[] MetaDataEncoding(DataMessageWithTopology<T> obj)
        {
            byte[] subscriptionBytes = ByteUtilities.StringToByteArrays(obj.SubscriptionName);
            var totalLengthUpdates = obj.TopologyUpdates.Sum(x => x.Size);
            byte[] buffer = new byte[sizeof(int) + totalLengthUpdates + sizeof(int) + subscriptionBytes.Length + sizeof(bool) + sizeof(int) + sizeof(int)];
            int offset = 0;

            Buffer.BlockCopy(BitConverter.GetBytes(subscriptionBytes.Length), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(subscriptionBytes, 0, buffer, offset, subscriptionBytes.Length);
            offset += subscriptionBytes.Length;

            Buffer.BlockCopy(BitConverter.GetBytes(obj.OperatorId), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(BitConverter.GetBytes(obj.Iteration), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(BitConverter.GetBytes(totalLengthUpdates), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);

            TopologyUpdate.Serialize(buffer, ref offset, obj.TopologyUpdates);
           
            return buffer;
        }

        private static Tuple<string, int, int, List<TopologyUpdate>> MetaDataDecoding(byte[] obj)
        {
            int offset = 0;
            int subscriptionLength = BitConverter.ToInt32(obj, offset);
            offset += sizeof(int);

            string subscriptionString = ByteUtilities.ByteArraysToString(obj, offset, subscriptionLength);
            offset += subscriptionLength;

            int operatorInt = BitConverter.ToInt32(obj, offset);
            offset += sizeof(int);

            int iterationInt = BitConverter.ToInt32(obj, offset);
            offset += sizeof(int);

            int length = BitConverter.ToInt32(obj, offset);
            offset += sizeof(int);

            var updates = TopologyUpdate.Deserialize(obj, length, offset);

            return new Tuple<string, int, int, List<TopologyUpdate>>(subscriptionString, operatorInt, iterationInt, updates);
        }
    }
}