﻿// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Org.Apache.REEF.Driver.Context;
using System;
using System.Globalization;

namespace Org.Apache.REEF.Network.Elastic
{
    /// <summary>
    /// Utility class.
    /// </summary>
    internal static class Utils
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
        /// Builds a task identifier out of a subscription(s) and an id.
        /// </summary>
        /// <param name="subscriptionName">The subscriptions active in the task</param>
        /// <param name="id">The task id</param>
        /// <returns>The task identifier</returns>
        public static string BuildTaskId(string subscriptionName, int id)
        {
            return BuildIdentifier("Task", subscriptionName, id);
        }

        /// <summary>
        /// Gets the context associated with the Task id.
        /// </summary>
        /// <param name="taskId">The task id to check</param>
        /// <returns>The context id associated with the task id</returns>
        public static string GetContextIdFromTaskId(string taskId)
        {
            return taskId.Replace("Task", "Context");
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
