using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    public class RingTaskMessageSource : ITaskMessageSource
    {
        private string _taskId;
        private readonly byte[] _message = ByteUtilities.StringToByteArrays(Constants.AggregationRing);

        [Inject]
        private RingTaskMessageSource()
        {
        }

        public void WaitingForToken(string taskId)
        {
            _taskId = taskId;
        }

        public Optional<TaskMessage> Message
        {
            get
            {
                if (_taskId == string.Empty || _taskId == null)
                {
                    return Optional<TaskMessage>.Empty();
                }
                else
                {
                    var defaultTaskMessage = TaskMessage.From(_taskId, _message);
                    _taskId = string.Empty;

                    return Optional<TaskMessage>.Of(defaultTaskMessage);
                }
            }
        }
    }
}
