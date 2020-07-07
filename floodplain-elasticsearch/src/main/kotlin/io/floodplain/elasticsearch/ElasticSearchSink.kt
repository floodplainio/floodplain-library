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
package io.floodplain.elasticsearch

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
import io.confluent.connect.elasticsearch.ElasticsearchSinkTask
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
import io.floodplain.streams.api.ProcessorName
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import java.util.Optional

private val logger = mu.KotlinLogging.logger {}

fun Stream.elasticSearchConfig(name: String, uri: String): ElasticSearchSinkConfig {
    val c = ElasticSearchSinkConfig(name, uri, this.context)
    return this.addSinkConfiguration(c) as ElasticSearchSinkConfig
}

class ElasticSearchSinkConfig(val name: String, val uri: String, val context: TopologyContext) :
    Config {
    var sinkTask: ElasticsearchSinkTask? = null
    val materializedConfigs: MutableList<MaterializedSink> = mutableListOf()
    var instantiatedSinkElements: Map<Topic, MutableList<FloodplainSink>>? = null

    override fun materializeConnectorConfig(topologyContext: TopologyContext): List<MaterializedSink> {
        return materializedConfigs
    }

    override fun sourceElements(): List<SourceTopic> {
        return emptyList()
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
    }

    override fun sinkTask(): Any? {
        return sinkTask
    }

    override fun instantiateSinkElements(topologyContext: TopologyContext) {
        instantiatedSinkElements = instantiateSinkConfig(topologyContext, this) { ElasticsearchSinkConnector() }
    }
    override fun sinkElements(): Map<Topic, MutableList<FloodplainSink>> {
        return instantiatedSinkElements ?: emptyMap()
    }
}

fun PartialStream.elasticSearchSink(sinkName: String, index: String, topicName: String, config: ElasticSearchSinkConfig): FloodplainSink {
    val sinkProcessorName = ProcessorName.from(sinkName)
    val topic = Topic.from(topicName)
    val sinkTransformer = SinkTransformer(Optional.of(sinkProcessorName), topic, false, Optional.empty(), false, true)
    addTransformer(Transformer(sinkTransformer))

    val sinkConfig = mapOf(
        "connector.class" to "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "connection.url" to config.uri,
        "tasks.max" to "1",
        "type.name" to "_doc",
        "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
        "key.converter" to "org.apache.kafka.connect.json.JsonConverter", // maps not supported by elasticsearch
        "topics" to topic.qualifiedString(config.context),
        "schema.ignore" to "true",
        "behavior.on.null.values" to "delete",
        "type.name" to "_doc")
    config.materializedConfigs.add(MaterializedSink(config.name, listOf(topic), sinkConfig))
    val conn = ElasticsearchSinkConnector()
    conn.start(sinkConfig)
    val task = conn.taskClass().getDeclaredConstructor().newInstance() as ElasticsearchSinkTask
    task.start(sinkConfig)
    config.sinkTask = task
    val sink = floodplainSinkFromTask(task, config)
    return sink
}
