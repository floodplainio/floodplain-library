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
package io.floodplain.kotlindsl.sink

import io.floodplain.kotlindsl.Config
import io.floodplain.kotlindsl.FloodplainSink
import io.floodplain.kotlindsl.InputReceiver
import io.floodplain.kotlindsl.MaterializedSink
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.SourceTopic
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.kotlindsl.floodplainSinkFromTask
import io.floodplain.kotlindsl.instantiateSinkConfig
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.sink.LogSinkConnector
import io.floodplain.sink.LogSinkTask
import io.floodplain.streams.api.ProcessorName
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import java.util.Optional

private val logger = mu.KotlinLogging.logger {}

fun Stream.logSinkConfig(name: String): LogSinkConfiguration {
    val logSinkConfig = LogSinkConfiguration(name, this)
    this.addSinkConfiguration(logSinkConfig)
    return logSinkConfig
}

class LogSinkConfiguration(val name: String, private val owner: Stream) : Config {
    val materializedConfigs: MutableList<MaterializedSink> = mutableListOf()
    var instantiatedSinkElements: Map<Topic, MutableList<FloodplainSink>>? = null

    override fun sourceElements(): List<SourceTopic> {
        TODO("Not yet implemented")
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
        TODO("Not yet implemented")
    }

    override fun materializeConnectorConfig(topologyContext: TopologyContext): List<MaterializedSink> {
        return materializedConfigs
    }

    override fun sinkTask(): Any? {
        return null
    }

    override fun instantiateSinkElements(topologyContext: TopologyContext) {
        instantiatedSinkElements = instantiateSinkConfig(topologyContext, this) { LogSinkConnector() }
    }

    override fun sinkElements(): Map<Topic, MutableList<FloodplainSink>> {
        return instantiatedSinkElements ?: emptyMap()
    }
}

fun PartialStream.logSink(sinkName: String, topicName: String, config: LogSinkConfiguration): FloodplainSink {
    val sinkProcessorName = ProcessorName.from(sinkName)
    val topic = Topic.from(topicName)
    val sinkTransformer = SinkTransformer(Optional.of(sinkProcessorName), topic, false, Optional.empty(), Topic.FloodplainKeyFormat.FLOODPLAIN_STRING, Topic.FloodplainBodyFormat.CONNECT_JSON)
    addTransformer(Transformer(sinkTransformer))

    val sinkConfig = mapOf(
        "connector.class" to "io.floodplain.sink.LogSinkConnector",
        "tasks.max" to "1",
        "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
        // "key.converter" to "org.apache.kafka.connect.json.JsonConverter", // maps not supported by elasticsearch
        "key.converter" to "org.apache.kafka.connect.storage.StringConverter", // "org.apache.kafka.connect.json.JsonConverter",

        "topics" to topicName,
        "schema.ignore" to "true",
        "type.name" to "_doc")
    config.materializedConfigs.add(MaterializedSink(config.name, listOf(topic), sinkConfig))
    val conn = LogSinkConnector()
    conn.start(sinkConfig)
    val task = conn.taskClass().getDeclaredConstructor().newInstance() as LogSinkTask
    task.start(sinkConfig)
    // config.sinkTask = task
    return floodplainSinkFromTask(task, config)
}
