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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Streaming Codec for the Group Communication Message
    /// </summary>
    internal sealed class DataMessageStreamingCodec<T> : IStreamingCodec<DataMessage<T>>
    {
        private readonly IStreamingCodec<T> _codec;

        /// <summary>
        /// Empty constructor to allow instantiation by reflection
        /// </summary>
        [Inject]
        private DataMessageStreamingCodec(IStreamingCodec<T> codec)
        {
            _codec = codec;
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <returns>The Group Communication Message</returns>
        public DataMessage<T> Read(IDataReader reader)
        {
            int metadataSize = reader.ReadInt32();
            byte[] metadata = new byte[metadataSize];
            reader.Read(ref metadata, 0, metadataSize);
            var res = GenerateMetaDataDecoding(metadata);

            string subscriptionName = res.Item1;
            int operatorId = res.Item2;
            var data = _codec.Read(reader);

            return new DataMessage<T>(subscriptionName, operatorId, data);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(DataMessage<T> obj, IDataWriter writer)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);
            byte[] encodedInt = BitConverter.GetBytes(encodedMetadata.Length);
            byte[] totalEncoding = encodedInt.Concat(encodedMetadata).ToArray();
            writer.Write(totalEncoding, 0, totalEncoding.Length);

            _codec.Write(obj.Data, writer);
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        /// <returns>The Group Communication Message</returns>
        public async Task<DataMessage<T>> ReadAsync(IDataReader reader,
            CancellationToken token)
        {
            int metadataSize = await reader.ReadInt32Async(token);
            byte[] metadata = new byte[metadataSize];
            await reader.ReadAsync(metadata, 0, metadataSize, token);
            var res = GenerateMetaDataDecoding(metadata);
            var data = await _codec.ReadAsync(reader, token);
            string subscriptionString = res.Item1;
            int operatorId = res.Item2;

            return new DataMessage<T>(subscriptionString, operatorId, data);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async System.Threading.Tasks.Task WriteAsync(DataMessage<T> obj, IDataWriter writer, CancellationToken token)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);
            byte[] encodedInt = BitConverter.GetBytes(encodedMetadata.Length);
            byte[] totalEncoding = encodedInt.Concat(encodedMetadata).ToArray();
            await writer.WriteAsync(totalEncoding, 0, totalEncoding.Length, token);

            await _codec.WriteAsync(obj.Data, writer, token);
        }

        private static byte[] GenerateMetaDataEncoding(DataMessage<T> obj)
        {
            List<byte[]> metadataBytes = new List<byte[]>();

            byte[] subscriptionBytes = ByteUtilities.StringToByteArrays(obj.SubscriptionName);
            byte[] operatorBytes = BitConverter.GetBytes(obj.OperatorId);

            metadataBytes.Add(BitConverter.GetBytes(subscriptionBytes.Length));
            metadataBytes.Add(subscriptionBytes);
            metadataBytes.Add(operatorBytes);

            return metadataBytes.SelectMany(i => i).ToArray();
        }

        private static Tuple<string, int> GenerateMetaDataDecoding(byte[] obj)
        {
            int subscriptionLength = BitConverter.ToInt32(obj, 0);
            int offset = sizeof(int);

            string subscriptionString = ByteUtilities.ByteArraysToString(obj.Skip(offset).Take(subscriptionLength).ToArray());
            offset += subscriptionLength;
            int operatorInt = BitConverter.ToInt32(obj, offset);

            return new Tuple<string, int>(subscriptionString, operatorInt);
        }
    }
}