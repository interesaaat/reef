using System;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Driver.Policy;
using Org.Apache.REEF.Network.Elastic.Topology.Impl;
using Org.Apache.REEF.Network.Elastic.Topology;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    class ElasticBroadcast : ElasticOperator
    {
        // private string _senderId;
        public ElasticBroadcast(string senderId, ElasticOperator prev, TopologyTypes topologyType, PolicyLevel policyLevel)
        {
            _prev = prev;
            _topology = topologyType == TopologyTypes.Flat ? (ITopology)new FlatTopology() : (ITopology)new TreeTopology();
            _policy = policyLevel;
        }
    }
}
