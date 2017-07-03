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
            string[] parts = activeContext.Id.Split('-');
            if (parts.Length != 3)
            {
                throw new ArgumentException("Invalid id in active context");
            }

            return int.Parse(parts[2], CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets the context number associated with the Active Context id.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>The context number associated with the active context id</returns>
        public static int GetTaskNum(string taskId)
        {
            string[] parts = taskId.Split('-');
            if (parts.Length != 2)
            {
                throw new ArgumentException("Invalid id in active context");
            }

            return int.Parse(parts[1], CultureInfo.InvariantCulture);
        }

        public static string BuildContextName(string subscriptionName, int contextNum)
        {
            return string.Format(CultureInfo.InvariantCulture, "TaskContext-{0}-{1}", subscriptionName, contextNum);
        }

        public static string BuildTaskId(string subscriptionName, int id)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}", subscriptionName, id);
        }

        /// <summary>
        /// Returns the TaskIdentifier from the Configuration.
        /// </summary>
        /// <param name="taskConfiguration">The Configuration object</param>
        /// <returns>The TaskIdentifier for the given Configuration</returns>
        public static bool GetIsMasterTask(IConfiguration taskConfiguration)
        {
            try
            {
                IInjector injector = TangFactory.GetTang().NewInjector(taskConfiguration);
                return injector.GetNamedInstance<ElasticConfig.IsMasterTask, bool>(
                    GenericType<ElasticConfig.IsMasterTask>.Class);
            }
            catch (InjectionException)
            {
                throw;
            }
        }
    }
}
