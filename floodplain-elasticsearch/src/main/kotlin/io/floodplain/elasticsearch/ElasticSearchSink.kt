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

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
import io.confluent.connect.elasticsearch.ElasticsearchSinkTask
import io.floodplain.kotlindsl.Config
import io.floodplain.kotlindsl.FloodplainSink
import io.floodplain.kotlindsl.InputReceiver
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.SourceTopic
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.ProcessorName
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import java.util.Optional
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

private val logger = mu.KotlinLogging.logger {}

fun Stream.elasticSearchConfig(name: String, uri: String): ElasticSearchSinkConfig {
    val c = ElasticSearchSinkConfig(name, uri, this.context, this)
    return this.addSinkConfiguration(c) as ElasticSearchSinkConfig
}

class ElasticSearchSinkConfig(val name: String, val uri: String, val context: TopologyContext, stream: Stream) :
    Config {
    var sinkTask: ElasticsearchSinkTask? = null
    val sinks: MutableMap<Topic, FloodplainSink> = mutableMapOf()

    override fun materializeConnectorConfig(): Pair<String, Map<String, String>> {
        return "" to emptyMap<String, String>()
    }

    override fun sourceElements(): List<SourceTopic> {
        return emptyList()
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
    }

    override fun sinkElements(): Map<Topic, FloodplainSink> {
        return sinks
    }

    override fun sinkTask(): Any? {
        return sinkTask
    }
}

// TODO, use generic one
private class ElasticSearchSink(private val topic: String, private val task: SinkTask, private val config: ElasticSearchSinkConfig) : FloodplainSink {
    val deserializer = JsonDeserializer()
    var mapper: ObjectMapper = ObjectMapper()

    private val offsetCounter = AtomicLong(System.currentTimeMillis())
    override fun send(topic: Topic, elements: List<Pair<String, Map<String, Any>?>>, topologyContext: TopologyContext) {
    // override fun send(docs: List<Triple<Topic, String, IMessage?>>) {
        val list = elements.map { (key, value) ->
            // logger.info("Sending document to elastic. Topic: $topic Key: $key message: $result")
            SinkRecord(topic.qualifiedString(topologyContext), 0, null, key, null, value, offsetCounter.incrementAndGet())
        }.toList()
        task.put(list)
    }

    override fun config(): Config {
        return config
    }

    override fun flush() {
        task.flush(emptyMap())
    }

    override fun close() {
        task.flush(emptyMap())
        task.close(mutableListOf())
    }
}

fun PartialStream.elasticSearchSink(sinkName: String, index: String, topicName: String, config: ElasticSearchSinkConfig): FloodplainSink {
    val sinkProcessorName = ProcessorName.from(sinkName)
    val topic = Topic.from(topicName)
    val sinkTransformer = SinkTransformer(Optional.of(sinkProcessorName), topic, false, Optional.empty(), true)
    addTransformer(Transformer(sinkTransformer))

    val sinkConfig = mapOf(
        "connector.class" to "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "connection.url" to config.uri,
        "tasks.max" to "1",
        "type.name" to "_doc",
        // "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
        "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
        // "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
        "topics" to topic.qualifiedString(config.context),
        "schema.ignore" to "true",
        "behavior.on.null.values" to "delete",
        "type.name" to "_doc")
    val conn = ElasticsearchSinkConnector()
    conn.start(sinkConfig)
    val task = conn.taskClass().getDeclaredConstructor().newInstance() as ElasticsearchSinkTask
    task.start(sinkConfig)
    config.sinkTask = task
    val sink = ElasticSearchSink(topic.qualifiedString(config.context), task, config)
    config.sinks[topic] = sink
    return sink
}
