using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    public enum PolicyLevel { IGNORE, STOP_OPERATOR_AND_RESUBMIT, STOP_OPERATOR_AND_RECOMPUTE, STOP_SUBSCRIPTION_AND_RESUBMIT, STOP_SUBSCRIPTION_AND_RECOMPUTE, STOP_THE_WORLD_AND_RESUBMIT, STOP_THE_WORLD_AND_RECOMPUTE };

    public enum TaskSetStatus { NOT_SCHEDULED, RUNNING, STOPPED, FINISHED };
}
