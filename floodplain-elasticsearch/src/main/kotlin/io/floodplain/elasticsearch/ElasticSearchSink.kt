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
import io.floodplain.kotlindsl.FloodplainSink
import io.floodplain.kotlindsl.MaterializedConfig
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.SinkConfig
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.kotlindsl.floodplainSinkFromTask
import io.floodplain.kotlindsl.instantiateSinkConfig
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.ProcessorName
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import java.util.Optional

fun Stream.elasticSearchConfig(name: String, uri: String): ElasticSearchSinkConfig {
    val c = ElasticSearchSinkConfig(name, uri, this.topologyContext)
    return this.addSinkConfiguration(c) as ElasticSearchSinkConfig
}

class ElasticSearchSinkConfig(val name: String, val uri: String, val context: TopologyContext) :
    SinkConfig {
    var sinkTask: ElasticsearchSinkTask? = null
    val materializedConfigs: MutableList<MaterializedConfig> = mutableListOf()
    private var instantiatedSinkElements: Map<Topic, MutableList<FloodplainSink>>? = null

    override fun materializeConnectorConfig(topologyContext: TopologyContext): List<MaterializedConfig> {
        return materializedConfigs
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

fun PartialStream.elasticSearchSink(sinkName: String, topicName: String, config: ElasticSearchSinkConfig): FloodplainSink {
    val sinkProcessorName = ProcessorName.from(sinkName)
    val topic = Topic.from(topicName,topologyContext)
    val sinkTransformer = SinkTransformer(Optional.of(sinkProcessorName), topic, false, Optional.empty(), Topic.FloodplainKeyFormat.FLOODPLAIN_STRING, Topic.FloodplainBodyFormat.CONNECT_JSON)
    addTransformer(Transformer(sinkTransformer,topologyContext))

    val sinkConfig = mapOf(
        "connector.class" to "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "connection.url" to config.uri,
        "tasks.max" to "1",
        "type.name" to "_doc",
        "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
        "key.converter" to "org.apache.kafka.connect.json.JsonConverter", // maps not supported by elasticsearch
        "topics" to topic.qualifiedString(),
        "schema.ignore" to "true",
        "behavior.on.null.values" to "delete",
        "type.name" to "_doc"
    )
    config.materializedConfigs.add(MaterializedConfig(config.name, listOf(topic), sinkConfig))
    val conn = ElasticsearchSinkConnector()
    conn.start(sinkConfig)
    val task = conn.taskClass().getDeclaredConstructor().newInstance() as ElasticsearchSinkTask
    task.start(sinkConfig)
    config.sinkTask = task
    return floodplainSinkFromTask(task, config)
}
