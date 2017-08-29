using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Task.Impl;
using System;

namespace Org.Apache.REEF.Network.Elastic.Topology.Task
{
    public abstract class DriverAwareOperatorTopology : OperatorTopology, IObserver<string>
    {
        internal DriverAwareOperatorTopology(string taskId, int rootId, string subscription, int timeout, int operatorId, CommunicationLayer commLayer)
            : base(taskId, rootId, subscription, timeout, operatorId, commLayer)
        {
        }

        public abstract void OnNext(string value);
    }
}
