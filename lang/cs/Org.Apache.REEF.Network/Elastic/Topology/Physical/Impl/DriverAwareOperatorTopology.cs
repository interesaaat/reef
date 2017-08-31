using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Tang.Exceptions;
using System;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    public abstract class DriverAwareOperatorTopology : OperatorTopology, IObserver<IDriverMessagePayload>
    {
        internal DriverAwareOperatorTopology(string taskId, int rootId, string subscription, int timeout, int operatorId, CommunicationLayer commLayer)
            : base(taskId, rootId, subscription, timeout, operatorId, commLayer)
        {
        }

        public void OnNext(IDriverMessagePayload message)
        {
            switch (message.MessageType)
            {
                case DriverMessageType.Failure:
                    OnFailureResponseMessageFromDriver(message);
                    break;
                default:
                    OnMessageFromDriver(message);
                    break;
            }
        }

        public abstract void OnMessageFromDriver(IDriverMessagePayload value);

        public void OnFailureResponseMessageFromDriver(IDriverMessagePayload value)
        {
        }
    }
}
