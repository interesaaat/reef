using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using System;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    public class HeartBeatReference
    {
        private readonly IHeartBeatManager _heartBeatManager;

        [Inject]
        public HeartBeatReference(IInjector subInjector)
        {
            _heartBeatManager = subInjector.GetInstance<IHeartBeatManager>();
        }

        public void Heartbeat(TaskStatusProto proto)
        {
            _heartBeatManager.OnNext(proto);
        }

        public void Heartbeat()
        {
            _heartBeatManager.OnNext();
        }
    }
}
