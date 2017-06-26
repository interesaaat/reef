using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    public interface ITaskSetStatusView
    {
        TaskSetStatus Status { get; }

        void reset();
    }
}
