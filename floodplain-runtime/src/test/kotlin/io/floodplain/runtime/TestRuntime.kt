/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.runtime

import com.xenomachina.argparser.ArgParser
import org.junit.Assert
import org.junit.Test

class TestRuntime {

    @Test
    fun testArgs() {
        val args = arrayOf("--kafka", "localhost:9092")
        ArgParser(args).parseInto(::LocalArgs).run {
            Assert.assertEquals("localhost:9092", kafka)
            Assert.assertNull(connect)
            Assert.assertFalse(force)
        }
        val argsWithConnect = arrayOf("--kafka", "localhost:9092", "--connect", "http://localhost:8083", "--force")
        ArgParser(argsWithConnect).parseInto(::LocalArgs).run {
            Assert.assertEquals("localhost:9092", kafka)
            Assert.assertEquals("http://localhost:8083", connect)
            Assert.assertTrue(force)
        }
    }
}
