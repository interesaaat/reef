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
    public class MiniBenchmarkScans
    {
        internal void RunMiniBenchmarkScans()
        {
            var rand = new Random();
            int n = 1024 * 1024 * 256;
            var model = new float[n];
            int degree = 4;

            for (int i = 0; i < n; i++)
            {
                model[i] = NextFloat(rand);
            }

            Console.WriteLine("Read 1GB C#");
            Stopwatch stop = Stopwatch.StartNew();
            float tmp;

            for (int i = 0; i < n; i++)
            {
                tmp = model[i];
            }

            stop.Stop();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("total scan time {0}", stop.ElapsedMilliseconds);
            System.Threading.Thread.Sleep(1000);

            Console.WriteLine("Read 1GB parallel C#");
            Task[] tasks = new Task[degree];
            stop.Restart();

            for (int i = 0; i < degree; i++)
            {
                var t = n / degree;
                tasks[i] = Task.Factory.StartNew((object obj) =>
                {
                    float innerTmp;
                    int index = (int)obj;
                    for (int j = 0; j < t; j++)
                    {
                        innerTmp = model[(index * t) + j];
                    }
                }, i);
            }

            Task.WaitAll(tasks);
            stop.Stop();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            Console.WriteLine("total scan time {0}", stop.ElapsedMilliseconds);
            System.Threading.Thread.Sleep(1000);
        }

        static float NextFloat(Random random)
        {
            double mantissa = (random.NextDouble() * 2.0) - 1.0;
            double exponent = Math.Pow(2.0, random.Next(-126, 128));
            return (float)(mantissa * exponent);
        }
    }
}
