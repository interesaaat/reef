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
using Org.Apache.REEF.Network.Examples.Client.Elastic;
using Org.Apache.REEF.Network.Examples.GroupCommunication;
using Org.Apache.REEF.Network.Examples.Client.Minibenchmarks;

namespace Org.Apache.REEF.Network.Examples.Client
{
    public class Run
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("start running client: " + DateTime.Now);
            bool runOnYarn = false;
            int numNodes = 30;
            int startPort = 8900;
            int portRange = 1000;
            string testToRun = "ElasticIterateBroadcast";
            testToRun = testToRun.ToLower();

            if (args != null)
            {
                if (args.Length > 0)
                {
                    runOnYarn = bool.Parse(args[0].ToLower());
                }

                if (args.Length > 1)
                {
                    numNodes = int.Parse(args[1]);
                }

                if (args.Length > 2)
                {
                    startPort = int.Parse(args[2]);
                }

                if (args.Length > 3)
                {
                    portRange = int.Parse(args[3]);
                }

                if (args.Length > 4)
                {
                    testToRun = args[4].ToLower();
                }
            }

            if (testToRun.Equals("RunPipelineBroadcastAndReduce".ToLower()) || testToRun.Equals("all"))
            {
                int arraySize = GroupTestConstants.ArrayLength;
                int chunkSize = GroupTestConstants.ChunkSize;

                if (args.Length > 5)
                {
                    arraySize = int.Parse(args[5]);
                    chunkSize = int.Parse(args[6]);
                }

                new PipelineBroadcastAndReduceClient().RunPipelineBroadcastAndReduce(runOnYarn, numNodes, startPort,
                    portRange, arraySize, chunkSize);
                Console.WriteLine("RunPipelineBroadcastAndReduce completed!!!");
            }

            if (testToRun.Equals("RunElasticBroadcast".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastClient().RunElasticBroadcast(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticRunBroadcast completed!!!");
            }

            if (testToRun.Equals("ElasticIterateBroadcast".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticIterateBroadcastClient().RunIterateBroadcast(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticIterateBroadcast completed!!!");
            }

            if (testToRun.Equals("ElasticIterateAggregate".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticIterateAggregateClient().RunIterateAggregate(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticIterateAggregate completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcastReduce".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastReduceClient().RunBroadcastReduce(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticRunBroadcastReduce completed!!!");
            }

            if (testToRun.Equals("ElasticIterateBroadcastReduce".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticIterateBroadcastReduceClient().RunIterateBroadcastReduce(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticIterateBroadcastReduce completed!!!");
            }

            if (testToRun.Equals("ElasticParameterServer".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticParameterServerClient().RunParameterServer(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticParameterServer completed!!!");
            }

            if (testToRun.Equals("MiniBenchmarkScans".ToLower()) || testToRun.Equals("MiniBenchmarks".ToLower()))
            {
                new MiniBenchmarkScans().RunMiniBenchmarkScans();
                Console.WriteLine("MinibenchamarkScans completed!!!");
            }

            if (testToRun.Equals("MiniBenchmarkSerialization".ToLower()) || testToRun.Equals("MiniBenchmarks".ToLower()))
            {
                new MiniBenchmarkSerialization().RunMiniBenchmarkSerialization();
                Console.WriteLine("MiniBenchmarkSerialization completed!!!");
            }

            if (testToRun.Equals("MiniBenchmarkSend".ToLower()) || testToRun.Equals("MiniBenchmarks".ToLower()))
            {
                new MiniBenchmarkSend().RunMiniBenchmarkSend();
                Console.WriteLine("MiniBenchmarkSend completed!!!");
            }

            if (testToRun.Equals("MiniBenchmarkSerSend".ToLower()) || testToRun.Equals("MiniBenchmarks".ToLower()))
            {
                new MiniBenchmarkSerSend().RunMiniBenchmarkSerSend();
                Console.WriteLine("MiniBenchmarkSerSend completed!!!");
            }

            Console.ReadLine();
        }
    }
}
