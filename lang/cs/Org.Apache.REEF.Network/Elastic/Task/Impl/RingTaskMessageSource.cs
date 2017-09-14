using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Common.Runtime.Evaluator;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    public class RingTaskMessageSource : ITaskMessageSource
    {
        private string _taskId;
        private string _taskIdWithToken;
        private int _iterationNumber;

        private readonly HeartBeatReference _heartBeatManager;

        private readonly byte[] _message1 = BitConverter.GetBytes((ushort)TaskMessageType.JoinTheRing);
        private readonly byte[] _message2 = BitConverter.GetBytes((ushort)TaskMessageType.TokenReceived);

        [Inject]
        private RingTaskMessageSource(HeartBeatReference heartBeatManager)
        {
            _heartBeatManager = heartBeatManager;

            _taskId = string.Empty;
            _taskIdWithToken = string.Empty;
        }

        public void JoinTheRing(string taskId)
        {
            _taskId = taskId;

            _heartBeatManager.Heartbeat();
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
                    List<byte[]> buffer = new List<byte[]>(2)
                    {
                        _message2,
                        BitConverter.GetBytes(_iterationNumber)
                    };
                    var message = TaskMessage.From(_taskIdWithToken, buffer.SelectMany(i => i).ToArray());
                    _taskIdWithToken = string.Empty;

                    return Optional<TaskMessage>.Of(message);
                }

                return Optional<TaskMessage>.Empty();
            }
        }
    }
}
