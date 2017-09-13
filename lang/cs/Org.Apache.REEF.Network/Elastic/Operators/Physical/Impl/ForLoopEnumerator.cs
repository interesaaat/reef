using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    public class ForLoopEnumerator : ElasticIteratorEnumerator<int>
    {
        private int _iterations;

        [Inject]
        private ForLoopEnumerator([Parameter(typeof(OperatorParameters.NumIterations))] int iterations)
        {
            _iterations = iterations;
            State = 0;
        }

        public override bool MoveNext()
        {
            var result = State < _iterations;

            if (result)
            {
                State++;
            }

            return result;
        }

        public override int Current
        {
            get { return State; }
        }
    }
}
