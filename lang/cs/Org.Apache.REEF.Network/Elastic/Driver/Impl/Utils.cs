using Org.Apache.REEF.Driver.Context;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    public static class Utils
    {
        /// <summary>
        /// Gets the context number associated with the Active Context id.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>The context number associated with the active context id</returns>
        public static int GetContextNum(IActiveContext activeContext)
        {
            string[] parts = activeContext.Id.Split('-');
            if (parts.Length != 2)
            {
                throw new ArgumentException("Invalid id in active context");
            }

            return int.Parse(parts[1], CultureInfo.InvariantCulture);
        }

        public static string GetTaskContextName(string subscriptionName, int contextNum)
        {
            return string.Format(CultureInfo.InvariantCulture, "TaskContext-{0}-{1}", subscriptionName, contextNum);
        }

        public static string BuildTaskId(string subscriptionName, string op)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", subscriptionName, op, new Random().Next());
        }
    }
}
