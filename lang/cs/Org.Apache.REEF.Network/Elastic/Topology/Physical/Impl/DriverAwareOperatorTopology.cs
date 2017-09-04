using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using System;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    internal abstract class DriverAwareOperatorTopology : OperatorTopology, IObserver<IDriverMessagePayload>
    {
        internal DriverAwareOperatorTopology(string taskId, int rootId, string subscription, int operatorId, CommunicationLayer commLayer, 
            int timeout)
            : base(taskId, rootId, subscription, operatorId, commLayer, timeout)
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

        internal abstract void OnMessageFromDriver(IDriverMessagePayload value);

        internal abstract void OnFailureResponseMessageFromDriver(IDriverMessagePayload value);
    }
}
