using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Network.Elastic.Topology.Task;
using Org.Apache.REEF.Network.Elastic.Topology.Task.Impl;
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
        string message = string.Empty;
        if (value.Message.IsPresent())
        {
            message = ByteUtilities.ByteArraysToString(value.Message.Value);

            var data = message.Split(':');
            
            if (data.Length != 3)
            {
                throw new IllegalStateException("Can not recognize message " + message);
            }

            DriverAwareOperatorTopology observer;
            _messageObservers.TryGetValue(data[0], out observer);

            if (observer == null)
            {
                throw new IllegalStateException("Observer for task " + data[0] + " not found");
            }

            switch (data[1])
            {
                case Constants.AggregationRing:
                    if (!(observer.GetType() == typeof(AggregationRingTopology)))
                    {
                        throw new IllegalStateException("Observer " + observer + " not appropriate for " + Constants.AggregationRing);
                    }
                    observer.OnNext(data[2]);
                    break;
                default:
                    throw new IllegalStateException("Driver message for operator " + data[1] + " not supported");
            }
        }
    }
}