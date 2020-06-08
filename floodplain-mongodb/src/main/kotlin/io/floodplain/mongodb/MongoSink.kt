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
package io.floodplain.mongodb

import com.mongodb.kafka.connect.MongoSinkConnector
import io.floodplain.kotlindsl.Config
import io.floodplain.kotlindsl.FloodplainSink
import io.floodplain.kotlindsl.InputReceiver
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.SourceTopic
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.ProcessorName
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import java.util.Optional
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

private val logger = mu.KotlinLogging.logger {}

class MongoConfig(val name: String, val uri: String, val database: String, private val topologyContext: TopologyContext) : Config {

    val sinkInstancePair: MutableList<Pair<String, Topic>> = mutableListOf()
    override fun materializeConnectorConfig(): Pair<String, Map<String, String>> {
        val additional = mutableMapOf<String, String>()
        sinkInstancePair.forEach { (key, value) -> additional.put("topic.override.${value.qualifiedString(topologyContext)}.collection", key) }
        logger.debug("Pairs: $sinkInstancePair")
        val collections: String = sinkInstancePair.map { e -> e.first }.joinToString(",")
        logger.debug("Collections: $collections")
        val topics: String = sinkInstancePair.map { (collection, topic) -> topic.qualifiedString(topologyContext) }.joinToString(",")
        logger.debug("Topics: $topics")

        val generationalDatabase = topologyContext.topicName(database)
        val settings = mutableMapOf("connector.class" to "com.mongodb.kafka.connect.MongoSinkConnector",
                "value.converter.schemas.enable" to "false",
                "key.converter.schemas.enable" to "false",
                "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "document.id.strategy" to "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
                "delete.on.null.values" to "true",
                "debug" to "true",
                "connection.uri" to uri,
                "database" to generationalDatabase,
                "collection" to collections,
                "topics" to topics)
        settings.putAll(additional)
        settings.forEach { (key, value) ->
            logger.info { "Setting: $key value: $value" }
        }
        return name to settings
    }

    override fun sourceElements(): List<SourceTopic> {
        return emptyList()
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
        throw UnsupportedOperationException("MongoSink can not be used as a source")
    }

    override fun sinkElements(): Map<Topic, FloodplainSink> {
        val (key, settings) = materializeConnectorConfig()
        val connector = MongoSinkConnector()
        connector.start(settings)

        val task = connector.taskClass().getDeclaredConstructor().newInstance() as SinkTask
        task.start(settings)
        val sink = MongoFloodplainSink(topologyContext, task)
        return sinkInstancePair.map { it.second to sink }.toMap()
    }
}

private class MongoFloodplainSink(private val topologyContext: TopologyContext, private val task: SinkTask) : FloodplainSink {
    private val offsetCounter = AtomicLong(System.currentTimeMillis())

    override fun send(docs: List<Triple<Topic, String, IMessage?>>) {

        val list = docs.map { (topic, key, value) ->
            SinkRecord(topic.qualifiedString(topologyContext), 0, null, mapOf(Pair("key", key)), null, value?.data(), offsetCounter.incrementAndGet())
        }.toList()
        task.put(list)
    }

    override fun flush() {
        task.flush(emptyMap())
    }

    override fun close() {
        task.close(emptyList())
    }
}
/**
 * Creates a config for this specific connector type, add the required params as needed. This config object will be passed
 * to all sink objects
 */
fun Stream.mongoConfig(name: String, uri: String, database: String): MongoConfig {
    val c = MongoConfig(name, uri, database, this.context)
    this.addSinkConfiguration(c)
    return c
}

fun PartialStream.mongoSink(collection: String, topicDefinition: String, config: MongoConfig) {
    val topic = Topic.from(topicDefinition)
    config.sinkInstancePair.add(collection to topic)
    val sinkName = ProcessorName.from(config.name)
    val sink = SinkTransformer(Optional.of(sinkName), topic, false, Optional.empty(), false)
    val transform = Transformer(sink)
    addTransformer(transform)
}