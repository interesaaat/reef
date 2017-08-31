﻿using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Driver.Impl;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities;
using System;
using System.Collections.Concurrent;

public class RingDriverMessageHandler : IDriverMessageHandler
{
    private readonly ConcurrentDictionary<string, DriverAwareOperatorTopology> _messageObservers =
            new ConcurrentDictionary<string, DriverAwareOperatorTopology>();

    [Inject]
    public RingDriverMessageHandler()
    {
    }

    public void RegisterOperatorTopologyForDriver(string taskDestinationId, DriverAwareOperatorTopology operatorObserver)
    {
        if (_messageObservers.ContainsKey(taskDestinationId))
        {
            throw new IllegalStateException("Task " + taskDestinationId + " already added among listeners");
        }

        _messageObservers.TryAdd(taskDestinationId, operatorObserver);
    }

    public void Handle(IDriverMessage value)
    {
        if (value.Message.IsPresent())
        {
            var message = DriverMessage.From(value.Message.Value);

            DriverAwareOperatorTopology observer;
            _messageObservers.TryGetValue(message.Destination, out observer);

            if (observer == null)
            {
                throw new IllegalStateException("Observer for task " + message.Destination + " not found");
            }

            observer.OnNext(message.Message);
        }
        else
        {
            throw new IllegalStateException("Received message with no payload");
        }
    }
}