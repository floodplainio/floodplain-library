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
package io.floodplain.kotlindsl

import io.floodplain.bufferTimeout
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.streams.api.Topic
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import org.junit.Test

class TestOutputFlow {

    @Test
    fun testChunkin() {
        val flow: Flow<Triple<Topic, String, IMessage?>> = flow {
            repeat(10) {
                val topic = Topic.from("TopicNr$it")
                repeat(100) { msgNr ->
                    val msg = empty().set("element", msgNr)
                    emit(Triple(topic, "key$msgNr", msg))
                }
            }
        }

        runBlocking {
            var initialCount = 0
            val count = flow.onEach { initialCount++ }
                .bufferTimeout(7, 1000)
                .map { elts -> elts.size }
                .fold(0, { acc, cnt -> acc + cnt })

            println("total: $count total: $initialCount")
        }
    }
}
