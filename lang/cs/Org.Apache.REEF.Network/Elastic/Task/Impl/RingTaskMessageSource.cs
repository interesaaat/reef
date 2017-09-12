using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    public class RingTaskMessageSource : ITaskMessageSource
    {
        private string _taskId;
        private string _taskIdWithToken;
        private int _iterationNumber;

        private readonly byte[] _message1 = BitConverter.GetBytes((ushort)TaskMessageType.JoinTheRing);
        private readonly byte[] _message2 = BitConverter.GetBytes((ushort)TaskMessageType.TokenReceived);

        [Inject]
        private RingTaskMessageSource()
        {
            _taskId = string.Empty;
            _taskIdWithToken = string.Empty;
        }

        public void JoinTheRing(string taskId)
        {
            _taskId = taskId;
        }

        public void TokenReceived(string taskId, int iterationNumber)
        {
            _taskIdWithToken = taskId;
            _iterationNumber = iterationNumber;
        }

        public Optional<TaskMessage> Message
        {
            get
            {
                if (_taskId != string.Empty)
                {
                    var message = TaskMessage.From(_taskId, _message1);
                    _taskId = string.Empty;

                    return Optional<TaskMessage>.Of(message);
                }
                if (_taskIdWithToken != string.Empty)
                {
                    List<byte[]> buffer = new List<byte[]>(2);
                    buffer.Add(_message2);
                    buffer.Add(BitConverter.GetBytes(_iterationNumber));
                    var message = TaskMessage.From(_taskIdWithToken, buffer.SelectMany(i => i).ToArray());
                    _taskIdWithToken = string.Empty;

                    return Optional<TaskMessage>.Of(message);
                }

                return Optional<TaskMessage>.Empty();
            }
        }
    }
}
