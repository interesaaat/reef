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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Examples.Client.Minibenchmarks
{
    public class MiniBenchmarkSerSend
    {
        internal void RunMiniBenchmarkSerSend()
        {
            var rand = new Random();
            int n = 1024 * 1024 * 256;
            int degree = 4;
            var block = 1024 * 32;
            var len = n * sizeof(float) / degree;
            var model = new float[n];
            var receivedModel = new float[n];
            var writeBuffer = new byte[n * sizeof(float)];
            var readBuffer = new byte[n * sizeof(float)];
            Stopwatch stop = Stopwatch.StartNew();

            for (int i = 0; i < n; i++)
            {
                model[i] = NextFloat(rand);
            }

            Console.WriteLine("Serialize and Send 1GB C#");
            Stream stream = new MemoryStream(1024 * 1024 * 1024);
            stop.Restart();

            Buffer.BlockCopy(model, 0, writeBuffer, 0, n * sizeof(float));
            stream.Write(writeBuffer, 0, n * sizeof(float));

            stop.Stop();
            stream.Seek(0, SeekOrigin.Begin);
            stream.Read(readBuffer, 0, n * sizeof(float));
            Buffer.BlockCopy(readBuffer, 0, receivedModel, 0, n * sizeof(float));
            Console.WriteLine(model.SequenceEqual(receivedModel));
            writeBuffer = new byte[n * sizeof(float)];
            readBuffer = new byte[n * sizeof(float)];
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("Send time {0}", stop.ElapsedMilliseconds);

            Console.WriteLine("Serialize and Send 1GB Blocks C#");
            stream = new MemoryStream(1024 * 1024 * 1024);
            byte[] smallBuffer = new byte[block];
            stop.Restart();

            for (int i = 0; i < n * sizeof(float); i += block)
            {
                Buffer.BlockCopy(model, i, smallBuffer, 0, block);
                stream.Write(smallBuffer, 0, block);
            }

            stop.Stop();
            stream.Seek(0, SeekOrigin.Begin);
            stream.Read(readBuffer, 0, n * sizeof(float));
            Buffer.BlockCopy(readBuffer, 0, receivedModel, 0, n * sizeof(float));
            Console.WriteLine(model.SequenceEqual(receivedModel));
            writeBuffer = new byte[n * sizeof(float)];
            readBuffer = new byte[n * sizeof(float)];
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("Send time {0}", stop.ElapsedMilliseconds);

            Console.WriteLine("Serialize and Send 1GB unsafe C#");
            stream = new MemoryStream(1024 * 1024 * 1024);
            stop.Restart();

            writeBuffer = GetBytes(model, writeBuffer);
            stream.Write(writeBuffer, 0, n * sizeof(float));

            stop.Stop();
            stream.Seek(0, SeekOrigin.Begin);
            stream.Read(readBuffer, 0, n * sizeof(float));
            Buffer.BlockCopy(readBuffer, 0, receivedModel, 0, n * sizeof(float));
            Console.WriteLine(model.SequenceEqual(receivedModel));
            writeBuffer = new byte[n * sizeof(float)];
            readBuffer = new byte[n * sizeof(float)];
            receivedModel = new float[n];
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("Send time {0}", stop.ElapsedMilliseconds);

            Console.WriteLine("Serialize and Send 1GB parallelize blocked C#");
            stream = new MemoryStream(1024 * 1024 * 1024);
            Task[] tasks = new Task[degree];
            stop.Restart();
            List<Task> tasks2 = new List<Task>();

            for (int i = 0; i < degree; i++)
            {
                tasks[i] = Task.Factory.StartNew((object obj) =>
                {
                    int index = (int)obj;
                    for (int j = 0; j < len; j += block)
                    {
                       Buffer.BlockCopy(model, (len * index) + j, writeBuffer, (len * index) + j, block);
                    }
                }, i);
            }

            Task.WaitAll(tasks);

            for (int i = 0; i < degree; i++)
            {
                tasks[i] = stream.WriteAsync(writeBuffer, len * i, len);
            }

            Task.WaitAll(tasks);

            stop.Stop();
            stream.Seek(0, SeekOrigin.Begin);
            stream.ReadAsync(readBuffer, 0, n * sizeof(float)).Wait();
            Buffer.BlockCopy(readBuffer, 0, receivedModel, 0, n * sizeof(float));
            Console.WriteLine(model.SequenceEqual(receivedModel));
            writeBuffer = new byte[n * sizeof(float)];
            readBuffer = new byte[n * sizeof(float)];
            receivedModel = new float[n];
            stream.Close();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("Send time {0}", stop.ElapsedMilliseconds);
        }

        static float NextFloat(Random random)
        {
            double mantissa = (random.NextDouble() * 2.0) - 1.0;
            double exponent = Math.Pow(2.0, random.Next(-126, 128));
            return (float)(mantissa * exponent);
        }

        unsafe static byte[] GetBytes(float[] value, byte[] buffer)
        {
            fixed (byte* b = buffer)
            fixed (float* v = value)
            {
                byte* pb = b;
                float* pv = v;
                for (int i = 0; i < value.Length; i++)
                {
                    *((int*)pb) = *(int*)pv;
                    pv++;
                    pb += 4;
                }
            }

            return buffer;
        }
    }
}
