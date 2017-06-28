using System;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Driver.Policy;
using Org.Apache.REEF.Network.Group.Topology;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    public abstract class ElasticOperator : FailureResponse
    {
        // For the moment we consider only linear sequences of operators (no branching for e.g., joins)
        protected ElasticOperator _next;
        protected ElasticOperator _prev;
        protected PolicyLevel _policy;
        protected ITopology _topology;

        public void AddTask(string taskId)
        {
            _topology.AddTask(taskId);

            if (_next != null)
            {
                _next.AddTask(taskId);
            }
        }

        public void EnsurePolicy(PolicyLevel level)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Broadcast<T>(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType, PolicyLevel policyLevel, params IConfiguration[] configurations)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Broadcast(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat, PolicyLevel policyLevel = PolicyLevel.IGNORE)
        {
            _next = new ElasticBroadcast(senderTaskId, prev, topologyType, policyLevel);
            return _next;
        }

        public void GetElasticTaskConfiguration(out ICsConfigurationBuilder confBuilder)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Iterate<T>(Delegate condition, ElasticOperator prev, params IConfiguration[] configurations)
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Reduce<T>(string receiverTaskId, ElasticOperator prev, TopologyTypes topologyType, params IConfiguration[] configurations)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public ElasticOperator Scatter(string senderTaskId, ElasticOperator prev, TopologyTypes topologyType = TopologyTypes.Flat)
        {
            throw new NotImplementedException();
        }

        public new void OnNext(IFailedEvaluator value)
        {
            _next.OnNext(value);
        }

        public new void OnNext(IFailedTask value)
        {
            _next.OnNext(value);
        }
    }
}
