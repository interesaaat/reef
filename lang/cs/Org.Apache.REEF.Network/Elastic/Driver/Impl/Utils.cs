﻿using Org.Apache.REEF.Driver.Context;
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
    /// <summary>
    /// Utility class.
    /// </summary>
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

        /// <summary>
        /// Gets the subscriptions associated with the Active Context id.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>The subscription names associated with the active context id</returns>
        public static string GetContextSubscriptions(IActiveContext activeContext)
        {
            return GetValue(1, activeContext.Id);
        }

        /// <summary>
        /// Gets the subscriptions associated with the Task id.
        /// </summary>
        /// <param name="taskId">The task id to check</param>
        /// <returns>The subscription names associated with the task id</returns>
        public static string GetTaskSubscriptions(string taskId)
        {
            return GetValue(1, taskId);
        }

        /// <summary>
        /// Gets the task number associated with the Task id.
        /// </summary>
        /// <param name="taskId">The task id to check</param>
        /// <returns>The task number associated with the task id</returns>
        public static int GetTaskNum(string taskId)
        {
            return int.Parse(GetValue(2, taskId), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Utility method returning a requested field out of an identifier
        /// </summary>
        /// <param name="field">The field of interest</param>
        /// <param name="identifier">The id to check</param>
        /// <returns>The field value extracted from the identifier</returns>
        private static string GetValue(int field, string identifer)
        {
            string[] parts = identifer.Split('-');
            if (parts.Length != 3 || field < 0 || field > 2)
            {
                throw new ArgumentException("Invalid identifier");
            }

            return parts[field];
        }

        /// <summary>
        /// Builds a context identifier out of a subscription(s) and a context number.
        /// </summary>
        /// <param name="subscriptionName">The subscriptions active in the context</param>
        /// <param name="contextNum">The context number</param>
        /// <returns>The context identifier</returns>
        public static string BuildContextId(string subscriptionName, int contextNum)
        {
            return BuildIdentifier("Context", subscriptionName, contextNum);
        }

        /// <summary>
        /// Builds a task identifier out of a subscription(s) and a task number.
        /// </summary>
        /// <param name="subscriptionName">The subscriptions active in the task</param>
        /// <param name="contextNum">The task number</param>
        /// <returns>The task identifier</returns>
        public static string BuildTaskId(string subscriptionName, int id)
        {
            return BuildIdentifier("Task", subscriptionName, id);
        }

        /// <summary>
        /// Utility method returning an identifier by merging the input fields
        /// </summary>
        /// <param name="first">The first field</param>
        /// <param name="second">The second field</param>
        /// <param name="third">The third field</param>
        /// <returns>An id merging the three fields</returns>
        private static string BuildIdentifier(string first, string second, int third)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", first, second, third);
        }
    }
}