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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using System.Threading;
using Org.Apache.REEF.Common.Tasks.Events;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public class IterateBroadcastMasterTask : ITask
    {
        private readonly IElasticTaskSetService _serviceClient;
        private readonly IElasticTaskSetSubscription _subscriptionClient;
        private readonly IElasticIterator<int> _iterator;
        private readonly IElasticBroadcast<int> _broadcastSender;

        private readonly CancellationTokenSource _cancellationSource;

        [Inject]
        public IterateBroadcastMasterTask(
            IElasticTaskSetService serviceClient)
        {
            _serviceClient = serviceClient;
            _cancellationSource = new CancellationTokenSource();

            _subscriptionClient = _serviceClient.GetSubscription("IterateBroadcast");
            _iterator = _subscriptionClient.GetIterator<int>(2);
            _broadcastSender = _subscriptionClient.GetBroadcast<int>(3);
        }

        public byte[] Call(byte[] memento)
        {
            _serviceClient.WaitForTaskRegistration(_cancellationSource);

            int number;

            var rand = new Random();

            while (_iterator.MoveNext())
            {
                number = rand.Next();

                _broadcastSender.Send(number);

                Console.WriteLine("Master has sent {0} in iteration {1}", number, _iterator.Current);
            }

            return null;
        }

        public void Handle(IDriverMessage message)
        {
        }

            public void Dispose()
        {
            _cancellationSource.Cancel();
            _serviceClient.Dispose();

            Console.WriteLine("Disposed.");
        }
    }
}
