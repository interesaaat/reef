using System;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Driver.Policy;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    class ElasticEmpty : ElasticOperator
    {
        public ElasticEmpty()
        {
            _policy = PolicyLevel.IGNORE;
        }
    }
}
