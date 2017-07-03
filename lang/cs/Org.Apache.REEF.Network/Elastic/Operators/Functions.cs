using Org.Apache.REEF.Network.Elastic.Operators.Logical;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Elastic.Operators
{
    public class SumFunction : IReduceFunction<int, int>
    { 
        public int Merge(IEnumerable<int> elements)
        {
            return Reduce(elements);
        }

        public int Reduce(IEnumerable<int> elements)
        {
            return elements.Sum();
        }
    }

    public class MaxFunction : SumFunction
    {
        public new int Reduce(IEnumerable<int> elements)
        {
            return elements.Max();
        }
    }
}
