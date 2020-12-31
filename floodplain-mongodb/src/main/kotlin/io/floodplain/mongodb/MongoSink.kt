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
import com.mongodb.kafka.connect.sink.MongoSinkTask
import io.floodplain.kotlindsl.FloodplainSink
import io.floodplain.kotlindsl.MaterializedConfig
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.SinkConfig
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.ProcessorName
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import java.util.Optional
import java.util.concurrent.atomic.AtomicLong
import kotlin.system.measureTimeMillis

private val logger = mu.KotlinLogging.logger {}

class MongoConfig(val name: String, val uri: String, val database: String) : SinkConfig {

    var sinkTask: MongoSinkTask? = null
    val sinkInstancePair: MutableList<Pair<String, Topic>> = mutableListOf()
    var instantiatedSinkElements: Map<Topic, List<FloodplainSink>>? = null

    override fun materializeConnectorConfig(topologyContext: TopologyContext): List<MaterializedConfig> {
        val additional = mutableMapOf<String, String>()
        sinkInstancePair.forEach { (key, value) -> additional.put("topic.override.${value.qualifiedString()}.collection", key) }
        logger.debug("Pairs: $sinkInstancePair")
        val collections: String = sinkInstancePair.joinToString(",") { e -> e.first }
        logger.debug("Collections: $collections")
        val topics: String =
            sinkInstancePair.joinToString(",") { (_, topic) -> topic.qualifiedString() }
        logger.debug("Topics: $topics")

        val generationalDatabase = topologyContext.topicName(database)
        val settings = mutableMapOf(
            "connector.class" to "com.mongodb.kafka.connect.MongoSinkConnector",
            "value.converter.schemas.enable" to "false",
            "key.converter.schemas.enable" to "false",
            "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
            // "key.converter" to "io.floodplain.kafka.converter.ReplicationMessageConverter",
            "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
            "document.id.strategy" to "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
            "delete.on.null.values" to "true",
            "debug" to "true",
            "connection.uri" to uri,
            "database" to generationalDatabase,
            "collection" to collections,
            "topics" to topics
        )
        settings.putAll(additional)
        settings.forEach { (key, value) ->
            logger.info { "Setting: $key value: $value" }
        }
        return listOf(MaterializedConfig(name, sinkInstancePair.map { (_, topic) -> topic }.toList(), settings))
    }

    override fun sinkElements(): Map<Topic, List<FloodplainSink>> {
        return instantiatedSinkElements ?: emptyMap()
    }
    override fun instantiateSinkElements(topologyContext: TopologyContext) {
        instantiatedSinkElements = materializeConnectorConfig(topologyContext)
            .map {

                val connector = MongoSinkConnector()
                logger.info("Mongo SEttings: ${it.settings}")
                connector.start(it.settings)
                val task = connector.taskClass().getDeclaredConstructor().newInstance() as MongoSinkTask
                task.start(it.settings)
                sinkTask = task

                // sinkInstancePair
                //     .map { (name,topic)-> topic to listOf(MongoFloodplainSink(task, this))}
                //     .toMap()
                val localSink = MongoFloodplainSink(task, this)
                it.topics
                    .map { topic -> topic to listOf(localSink) }
                    .toMap()

                // name to MongoFloodplainSink(task, this)
            }.first() // I know there is only one
    }

    override fun sinkTask(): Any? {
        return sinkTask
    }
}

private class MongoFloodplainSink(private val task: SinkTask, private val config: SinkConfig) : FloodplainSink {
    private val offsetCounter = AtomicLong(System.currentTimeMillis())

    override fun send(topic: Topic, elements: List<Pair<String, Map<String, Any>?>>, topologyContext: TopologyContext) {
        val list = elements.map { (key, value) ->
            SinkRecord(topic.qualifiedString(), 0, null, key, null, value, offsetCounter.incrementAndGet())
        }.toList()
        val insertTime = measureTimeMillis {
            try {
                task.put(list)
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }
        logger.info("Inserting into mongodb size: ${list.size} duration: $insertTime")
    }

    override fun config(): SinkConfig {
        return config
    }

    override fun flush() {
        task.flush(emptyMap())
    }

    override fun close() {
        task.close(emptyList())
    }

    override fun taskObject(): Any? {
        TODO("Not yet implemented")
    }
}
/**
 * Creates a config for this specific connector type, add the required params as needed. This config object will be passed
 * to all sink objects
 */
fun Stream.mongoConfig(name: String, uri: String, database: String): MongoConfig {
    val c = MongoConfig(name, uri, database)
    this.addSinkConfiguration(c)
    return c
}

fun PartialStream.mongoSink(collection: String, topicDefinition: String, config: MongoConfig) {
    val topic = Topic.from(topicDefinition,topologyContext)
    config.sinkInstancePair.add(collection to topic)
    val sinkName = ProcessorName.from(config.name)
    val sink = SinkTransformer(Optional.of(sinkName), topic, false, Optional.empty(), Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON)
    val transform = Transformer(rootTopology, sink, topologyContext)
    addTransformer(transform)
}
