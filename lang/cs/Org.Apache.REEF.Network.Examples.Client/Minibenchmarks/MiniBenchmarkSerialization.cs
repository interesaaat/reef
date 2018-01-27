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
using System.Diagnostics;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Examples.Client.Minibenchmarks
{
    public class MiniBenchmarkSerialization
    {
        internal void RunMiniBenchmarkSerialization()
        {
            var rand = new Random();
            int n = 1024 * 1024 * 256;
            int degree = 4;
            var block = 1024 * 32;
            var model = new float[n];
            var receivedModel = new float[n];
            var writeBuffer = new byte[n * sizeof(float)];
            Task[] tasks = new Task[degree];
            var len = n / degree * sizeof(float);

            for (int i = 0; i < n; i++)
            {
                model[i] = NextFloat(rand);
            }

            Console.WriteLine("Serialize 1GB C#");
            Stopwatch stop = Stopwatch.StartNew();

            Buffer.BlockCopy(model, 0, writeBuffer, 0, n * sizeof(float));

            stop.Stop();
            writeBuffer = new byte[n * sizeof(float)];
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("total serialization time {0}", stop.ElapsedMilliseconds);
            System.Threading.Thread.Sleep(1000);

            Console.WriteLine("Serialize 1GB parallel C#");

            stop.Restart();

            for (int i = 0; i < degree; i++)
            {
                tasks[i] = Task.Factory.StartNew((object obj) =>
                {
                    int index = (int)obj;
                    Buffer.BlockCopy(model, len * index, writeBuffer, len * index, len);
                }, i);
            }

            Task.WaitAll(tasks);

            stop.Stop();
            writeBuffer = new byte[n * sizeof(float)];
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("total serialization time {0}", stop.ElapsedMilliseconds);
            System.Threading.Thread.Sleep(1000);

            Console.WriteLine("Serialize 1GB blocks C#");
            stop.Restart();

            for (int i = 0; i < n * sizeof(float); i += block)
            {
                Buffer.BlockCopy(model, i, writeBuffer, i, block);
            }

            stop.Stop();
            writeBuffer = new byte[n * sizeof(float)];
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("total serialization time {0}", stop.ElapsedMilliseconds);

            Console.WriteLine("Serialize 1GB unsafe C#");
            stop.Restart();

            writeBuffer = GetBytes(model, writeBuffer);

            stop.Stop();
            writeBuffer = new byte[n * sizeof(float)];
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("Send time {0}", stop.ElapsedMilliseconds);

            Console.WriteLine("Serialize 1GB parallel in blocks C#");
            tasks = new Task[degree];
            stop.Restart();

            for (int i = 0; i < degree; i++)
            {
                tasks[i] = Task.Factory.StartNew((object obj) =>
                {
                    for (int j = 0; j < len; j += block)
                    {
                        int index = (int)obj;
                        Buffer.BlockCopy(model, (len * index) + j, writeBuffer, (len * index) + j, block);
                    }
                }, i);
            }

            Task.WaitAll(tasks);

            stop.Stop();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("total serialization time {0}", stop.ElapsedMilliseconds);
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
