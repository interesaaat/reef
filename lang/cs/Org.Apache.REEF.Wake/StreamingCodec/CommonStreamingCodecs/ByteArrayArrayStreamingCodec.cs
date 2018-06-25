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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs
{
    /// <summary>
    /// Streaming codec for float array
    /// </summary>
    public sealed class ByteArrayArrayStreamingCodec : IStreamingCodec<byte[][]>
    {
        /// <summary>
        /// Injectable constructor
        /// </summary>
        [Inject]
        private ByteArrayArrayStreamingCodec()
        {
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The float array read from the reader</returns>
        public byte[][] Read(IDataReader reader)
        {
            int length = reader.ReadInt32();
            byte[][] buffer = new byte[length][];

            for (int i = 0; i < buffer.Length; i++)
            {
                length = reader.ReadInt32();
                buffer[i] = new byte[length];
                reader.Read(ref buffer[i], 0, length);
            }

            return buffer;
        }

        /// <summary>
        /// Writes the float array to the writer.
        /// </summary>
        /// <param name="obj">The float array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(byte[][] obj, IDataWriter writer)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "byte array is null");
            }

            writer.WriteInt32(obj.Length);

            for (int i = 0; i < obj.Length; i++)
            {
                if (obj == null)
                {
                    throw new ArgumentNullException("obj[i]", "byte array is null");
                }

                writer.WriteInt32(obj[i].Length);
                writer.Write(obj[i], 0, obj[i].Length);
            }           
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The float array read from the reader</returns>
        public async Task<byte[][]> ReadAsync(IDataReader reader, CancellationToken token)
        {
            int length = await reader.ReadInt32Async(token);
            byte[][] buffer = new byte[length][];

            for (int i = 0; i < buffer.Length; i++)
            {
                length = await reader.ReadInt32Async(token);
                buffer[i] = new byte[length];
                await reader.ReadAsync(buffer[i], 0, length, token);
            }

            return buffer;
        }

        /// <summary>
        /// Writes the float array to the writer.
        /// </summary>
        /// <param name="obj">The float array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        public async Task WriteAsync(byte[][] obj, IDataWriter writer, CancellationToken token)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "float array is null");
            }

            await writer.WriteInt32Async(obj.Length, token);

            for (int i = 0; i < obj.Length; i++)
            {
                if (obj == null)
                {
                    throw new ArgumentNullException("obj[i]", "byte array is null");
                }

                await writer.WriteInt32Async(obj[i].Length, token);
                await writer.WriteAsync(obj[i], 0, obj[i].Length, token);
            }
        }
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
    }
}
