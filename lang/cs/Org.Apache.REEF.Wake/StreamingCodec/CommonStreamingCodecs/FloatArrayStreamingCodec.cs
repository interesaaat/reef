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
    public sealed class FloatArrayStreamingCodec : IStreamingCodec<float[]>
    {
        private const int MAX_SIZE = 1024 * 256;

        /// <summary>
        /// Injectable constructor
        /// </summary>
        [Inject]
        private FloatArrayStreamingCodec()
        {
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The float array read from the reader</returns>
        public float[] Read(IDataReader reader)
        {
            int length = reader.ReadInt32();
            byte[] buffer = new byte[sizeof(float) * length];
            reader.Read(ref buffer, 0, buffer.Length);
            float[] floatArr = new float[length];
            Buffer.BlockCopy(buffer, 0, floatArr, 0, buffer.Length);
            return floatArr;
        }

        /// <summary>
        /// Writes the float array to the writer.
        /// </summary>
        /// <param name="obj">The float array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(float[] obj, IDataWriter writer)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "float array is null");
            }

            writer.WriteInt32(obj.Length);
            byte[] buffer = new byte[sizeof(float) * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, buffer.Length);
            writer.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The float array read from the reader</returns>
        public async Task<float[]> ReadAsync(IDataReader reader, CancellationToken token)
        {
            int length = await reader.ReadInt32Async(token);
            float[] floatArr = new float[length];
            length *= sizeof(float);

            if (length > MAX_SIZE)
            {
                var total = 0;
                var toRead = 0;
                var _readBuffer = new byte[MAX_SIZE];

                while (total < length)
                {
                    toRead = Math.Min(length - total, MAX_SIZE);
                    await reader.ReadAsync(_readBuffer, 0, toRead, token);
                    Buffer.BlockCopy(_readBuffer, 0, floatArr, total, toRead);
                    total += toRead;
                }
            }
            else
            {
                var _readBuffer = new byte[length];
                await reader.ReadAsync(_readBuffer, 0, length, token);
                Buffer.BlockCopy(_readBuffer, 0, floatArr, 0, length);
            }
            return floatArr;
        }

        /// <summary>
        /// Writes the float array to the writer.
        /// </summary>
        /// <param name="obj">The float array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        public async Task WriteAsync(float[] obj, IDataWriter writer, CancellationToken token)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "float array is null");
            }
            var length = obj.Length * sizeof(float);
            await writer.WriteInt32Async(obj.Length, token);

            if (length > MAX_SIZE)
            {
                var total = 0;
                var toWrite = 0;
                var _writeBuffer = new byte[MAX_SIZE];

                while (total < length)
                {
                    toWrite = Math.Min(length - total, MAX_SIZE);
                    Buffer.BlockCopy(obj, total, _writeBuffer, 0, toWrite);
                    writer.WriteAsync(_writeBuffer, 0, toWrite, token);
                    total += toWrite;
                }
            }
            else
            {
                var _writeBuffer = new byte[length];
                Buffer.BlockCopy(obj, 0, _writeBuffer, 0, length);
                writer.WriteAsync(_writeBuffer, 0, length, token);
            }
        }
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
    }
}
