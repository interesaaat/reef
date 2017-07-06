using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
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
            return int.Parse(GetValue(2, activeContext.Id), CultureInfo.InvariantCulture);
        }

        public static string GetContextSubscriptions(IActiveContext activeContext)
        {
            return GetValue(1, activeContext.Id);
        }

        /// <summary>
        /// Gets the context number associated with the Active Context id.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>The context number associated with the active context id</returns>
        public static int GetTaskNum(string taskId)
        {
            return int.Parse(GetValue(2, taskId), CultureInfo.InvariantCulture);
        }

        private static string GetValue(int value, string identifer)
        {
            string[] parts = identifer.Split('-');
            if (parts.Length != 3 || value < 0 || value > 2)
            {
                throw new ArgumentException("Invalid id in active context");
            }

            return parts[value];
        }

        public static string BuildContextName(string subscriptionName, int contextNum)
        {
            return BuildIdentifier("Context", subscriptionName, contextNum);
        }

        public static string BuildTaskId(string subscriptionName, int id)
        {
            return BuildIdentifier("Task", subscriptionName, id);
        }

        private static string BuildIdentifier(string first, string second, int third)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", first, second, third);
        }
    }
}
