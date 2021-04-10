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

import io.floodplain.kotlindsl.AbstractSinkConfig
import io.floodplain.kotlindsl.FloodplainSink
import io.floodplain.kotlindsl.MaterializedConfig
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import java.util.Optional

fun Stream.logSinkConfig(name: String): LogSinkConfiguration {
    val logSinkConfig = LogSinkConfiguration(topologyContext, topologyConstructor, name)
    this.addSinkConfiguration(logSinkConfig)
    return logSinkConfig
}

class LogSinkConfiguration(override val topologyContext: TopologyContext, override val topologyConstructor: TopologyConstructor, val name: String) : AbstractSinkConfig() {
    val materializedConfigs: MutableList<MaterializedConfig> = mutableListOf()
    private var instantiatedSinkElements: Map<Topic, MutableList<FloodplainSink>>? = null

    override fun materializeConnectorConfig(): List<MaterializedConfig> {
        return materializedConfigs
    }

    override fun sinkTask(): Any? {
        return null
    }

    override fun sinkElements(): Map<Topic, MutableList<FloodplainSink>> {
        return instantiatedSinkElements ?: emptyMap()
    }
}

fun PartialStream.logSink(sinkName: String, topicName: String, config: LogSinkConfiguration) {
    // val sinkProcessorName = ProcessorName.from(sinkName)
    val topic = Topic.fromQualified(   topicName, topologyContext)
    rootTopology.topologyConstructor.addDesiredTopic(topic, Optional.empty())
    val sinkTransformer = SinkTransformer(Optional.of(sinkName), topic, Optional.empty(), Topic.FloodplainKeyFormat.FLOODPLAIN_STRING, Topic.FloodplainBodyFormat.CONNECT_JSON)
    addTransformer(Transformer(rootTopology, sinkTransformer, topologyContext))

    val sinkConfig = mapOf(
        "connector.class" to "io.floodplain.sink.LogSinkConnector",
        "tasks.max" to "1",
        "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
        "key.converter" to "org.apache.kafka.connect.storage.StringConverter", // "org.apache.kafka.connect.json.JsonConverter",
        "topics" to topicName,
        "schema.ignore" to "true",
        "type.name" to "_doc"
    )
    config.materializedConfigs.add(MaterializedConfig(config.name, listOf(topic), sinkConfig))
    // val conn = LogSinkConnector()
    // conn.start(sinkConfig)
    // val task = conn.taskClass().getDeclaredConstructor().newInstance() as LogSinkTask
    // task.start(sinkConfig)
    // return floodplainSinkFromTask(task, config)
}
