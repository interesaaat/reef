// Licensed to the Apache Software Foundation (ASF) under one
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

using System;
using System.Runtime.Serialization;

namespace Org.Apache.REEF.Network.Elastic.Failures.Impl
{
    /// <summary>
    /// A serializable exception that represents a task application error.
    /// </summary>
    [Serializable]
    public sealed class OperatorException : Exception
    {
        private int _id;

        public int OperatorId { get; }

        /// <summary>
        /// Constructor. A serializable exception object that represents a task application error.
        /// All the application related error should be captured in this type of exception.
        /// When driver receives this type of exception, the system is not going to recover. 
        /// </summary>
        public OperatorException(string message, int id)
            : base(GetMessagePrefix(id) + message)
        {
            _id = id;
        }

        /// <summary>
        /// Constructor. A serializable exception object that represents a task application error and wraps an inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public OperatorException(string message, int id, Exception innerException)
            : base(GetMessagePrefix(id) + message, innerException)
        {
            _id = id;
        }

        public OperatorException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        private static string GetMessagePrefix(int id)
        {
            return "Operator " + id + " : ";
        }
    }
}