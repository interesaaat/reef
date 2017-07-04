using Org.Apache.REEF.Network.Elastic.Driver.TaskSet;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    public interface ITaskSetStatusView
    {
        TaskSetStatus Status { get; }

        void Reset();
    }
}
