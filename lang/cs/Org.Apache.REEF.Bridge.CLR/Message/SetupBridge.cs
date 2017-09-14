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
//<auto-generated />
namespace org.apache.reef.bridge.message
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using Microsoft.Hadoop.Avro;

    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.bridge.message.SetupBridge.
    /// </summary>
    [DataContract(Namespace = "org.apache.reef.bridge.message")]
    public partial class SetupBridge
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.bridge.message.SetupBridge"",""doc"":""Notify the C# bridge of the http port of the Java bridge webserver."",""fields"":[{""name"":""httpServerPortNumber"",""doc"":""The Java bridge http server port number."",""type"":""int""}]}";

        /// <summary>
        /// Gets the schema.
        /// </summary>
        public static string Schema
        {
            get
            {
                return JsonSchema;
            }
        }
      
        /// <summary>
        /// Gets or sets the httpServerPortNumber field.
        /// </summary>
        [DataMember]
        public int httpServerPortNumber { get; set; }
                
        /// <summary>
        /// Initializes a new instance of the <see cref="SetupBridge"/> class.
        /// </summary>
        public SetupBridge()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SetupBridge"/> class.
        /// </summary>
        /// <param name="httpServerPortNumber">The httpServerPortNumber.</param>
        public SetupBridge(int httpServerPortNumber)
        {
            this.httpServerPortNumber = httpServerPortNumber;
        }
    }
}
