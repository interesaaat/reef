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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Failures;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Streaming Codec for the Group Communication Message
    /// </summary>
    internal sealed class CheckpointMessageStreamingCodec<T> : IStreamingCodec<CheckpointMessage>
    {
        private readonly IStreamingCodec<T> _codec;
        private readonly ICheckpointState _checkpoint;

        /// <summary>
        /// Empty constructor to allow instantiation by reflection
        /// </summary>
        [Inject]
        private CheckpointMessageStreamingCodec(IStreamingCodec<T> codec, ICheckpointState checkpoint)
        {
            _codec = codec;
            _checkpoint = checkpoint;
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <returns>The Group Communication Message</returns>
        public CheckpointMessage Read(IDataReader reader)
        {
            int metadataSize = reader.ReadInt32() + sizeof(int) + sizeof(int);
            byte[] metadata = new byte[metadataSize];
            reader.Read(ref metadata, 0, metadataSize);
            var res = GenerateMetaDataDecoding(metadata, metadataSize - sizeof(int) - sizeof(int));

            string subscriptionName = res.Item1;
            int operatorId = res.Item2;
            int iteration = res.Item3;
            var data = _codec.Read(reader);
            var payload = _checkpoint.Create(iteration, data);

            payload.SubscriptionName = subscriptionName;
            payload.OperatorId = operatorId;

            return new CheckpointMessage(payload);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(CheckpointMessage obj, IDataWriter writer)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);
   
            writer.Write(encodedMetadata, 0, encodedMetadata.Length);

            _codec.Write((T)obj.Checkpoint.State, writer);
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        /// <returns>The Group Communication Message</returns>
        public async Task<CheckpointMessage> ReadAsync(IDataReader reader,
            CancellationToken token)
        {
            int metadataSize = reader.ReadInt32() + sizeof(int) + sizeof(int);
            byte[] metadata = new byte[metadataSize];
            await reader.ReadAsync(metadata, 0, metadataSize, token);
            var res = GenerateMetaDataDecoding(metadata, metadataSize - sizeof(int) - sizeof(int));
            
            var data = await _codec.ReadAsync(reader, token);
            int iteration = res.Item3;
            var payload = _checkpoint.Create(iteration, data);
            payload.SubscriptionName = res.Item1;
            payload.OperatorId = res.Item2;

            return new CheckpointMessage(payload);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async System.Threading.Tasks.Task WriteAsync(CheckpointMessage obj, IDataWriter writer, CancellationToken token)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);

            await writer.WriteAsync(encodedMetadata, 0, encodedMetadata.Length, token);

            await _codec.WriteAsync((T)obj.Checkpoint.State, writer, token);
        }

        private static byte[] GenerateMetaDataEncoding(CheckpointMessage obj)
        {
            byte[] subscriptionBytes = ByteUtilities.StringToByteArrays(obj.SubscriptionName);
            var length = subscriptionBytes.Length;
            byte[] metadataBytes = new byte[sizeof(int) + length + sizeof(int) + sizeof(int)];
            int offset = 0;

            Buffer.BlockCopy(BitConverter.GetBytes(length), 0, metadataBytes, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(subscriptionBytes, 0, metadataBytes, offset, length);
            offset += length;

            Buffer.BlockCopy(BitConverter.GetBytes(obj.OperatorId), 0, metadataBytes, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(BitConverter.GetBytes(obj.Checkpoint.Iteration), 0, metadataBytes, offset, sizeof(int));

            return metadataBytes;
        }

        private static Tuple<string, int, int> GenerateMetaDataDecoding(byte[] obj, int subscriptionLength)
        {
            int offset = 0;
            string subscriptionString = ByteUtilities.ByteArraysToString(obj, offset, subscriptionLength);
            offset += subscriptionLength;

            int operatorInt = BitConverter.ToInt32(obj, offset);
            offset += sizeof(int);

            int iteration = BitConverter.ToInt32(obj, offset);

            return new Tuple<string, int, int>(subscriptionString, operatorInt, iteration);
        }
    }
}