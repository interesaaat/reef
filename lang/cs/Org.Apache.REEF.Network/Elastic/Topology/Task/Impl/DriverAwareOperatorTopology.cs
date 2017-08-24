using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Elastic.Topology.Task.Impl;
using System;

namespace Org.Apache.REEF.Network.Elastic.Topology.Task
{
    public abstract class DriverAwareOperatorTopology : OperatorTopology, IObserver<string>
    {
        internal DriverAwareOperatorTopology(string taskId, int rootId, string subscription)
            : base(taskId, rootId, subscription)
        {
        }

        public abstract void OnNext(string value);
    }
}
