using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    public class FailureResponse : IObserver<IFailedEvaluator>, IObserver<IFailedTask>
    {
        public void OnNext(IFailedEvaluator value)
        {
            throw new NotImplementedException();
        }

        public void OnNext(IFailedTask value)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }
}
