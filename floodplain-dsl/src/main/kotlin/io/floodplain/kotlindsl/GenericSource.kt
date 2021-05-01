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

import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.reactive.source.topology.CustomTopicSource
import io.floodplain.replication.factory.ReplicationFactory
import org.apache.kafka.common.serialization.Serdes

fun PartialStream.genericSource(topic: String, parseMessage: (ByteArray) -> IMessage?, init: Source.() -> Unit = {}): Source {
    return genericSource(
        topic,
        { Serdes.String().deserializer().deserialize(topic, it) },
        parseMessage,
        this.rootTopology,
        init
    )
}

fun Stream.genericSource(topic: String, parseMessage: (ByteArray) -> IMessage?, init: Source.() -> Unit = {}) {
    addSource(genericSource(topic, { Serdes.String().deserializer().deserialize(topic, it) }, parseMessage, this, init))
}

private fun genericSource(
    topic: String,
    keyParse: (ByteArray) -> String,
    messageParse: (ByteArray) -> IMessage?,
    rootTopology: Stream,
    init: Source.() -> Unit = {}
): Source {
    val sourceElement = CustomTopicSource(
        topic,
        { data -> keyParse(data) },
        { data ->
            messageParse(data)?.toImmutable()?.let { ReplicationFactory.standardMessage(it) }
        }
    )
    val source = Source(rootTopology, sourceElement, rootTopology.topologyContext)
    source.init()
    return source
}
