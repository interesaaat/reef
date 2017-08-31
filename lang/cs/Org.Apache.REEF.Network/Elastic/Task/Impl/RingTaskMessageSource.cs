﻿using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using System;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    public class RingTaskMessageSource : ITaskMessageSource
    {
        private string _taskId;
        private string _taskIdWithToken;
        private readonly byte[] _message1 = BitConverter.GetBytes((ushort)RingTaskMessageType.WaitForToken);
        private readonly byte[] _message2 = BitConverter.GetBytes((ushort)RingTaskMessageType.TokenReceived);

        [Inject]
        private RingTaskMessageSource()
        {
            _taskId = string.Empty;
            _taskIdWithToken = string.Empty;
        }

        public void WaitingForToken(string taskId)
        {
            _taskId = taskId;
        }

        public void TokenReceived(string taskId)
        {
            _taskIdWithToken = taskId;
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
                    var message = TaskMessage.From(_taskIdWithToken, _message2);
                    _taskIdWithToken = string.Empty;

                    return Optional<TaskMessage>.Of(message);
                }

                return Optional<TaskMessage>.Empty();
            }
        }
    }
}
