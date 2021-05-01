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

import io.floodplain.kotlindsl.AbstractSinkConfig
import io.floodplain.kotlindsl.MaterializedConfig
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import java.util.Optional

private val logger = mu.KotlinLogging.logger {}

class MongoConfig(
    override val topologyContext: TopologyContext,
    override val topologyConstructor: TopologyConstructor,
    val name: String,
    private val uri: String,
    private val database: String
) :
    AbstractSinkConfig() {

    val sinkInstancePair: MutableList<Pair<String, Topic>> = mutableListOf()

    override fun materializeConnectorConfig(): List<MaterializedConfig> {
        val additional = mutableMapOf<String, String>()
        sinkInstancePair.forEach { (_, value) -> topologyConstructor.addDesiredTopic(value, Optional.empty()) }
        sinkInstancePair.forEach { (key, value) ->
            additional["topic.override.${value.qualifiedString()}.collection"] =
                key
        }
        val collections: String = sinkInstancePair.joinToString(",") { e -> e.first }
        val topics: String =
            sinkInstancePair.joinToString(",") { (_, topic) -> topic.qualifiedString() }
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
            "topic.creation.default.replication.factor" to "3",
            "topic.creation.default.partitions" to "1",
            "topic.creation.default.cleanup.policy" to "compact",
            "topic.creation.default.compression.type" to "lz4",
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

    override fun sinkTask(): Any? {
        return sinkTask
    }
}

/**
 * Creates a config for this specific connector type, add the required params as needed. This config object will be passed
 * to all sink objects
 */
fun Stream.remoteMongoConfig(name: String, uri: String, database: String): MongoConfig {
    val c = MongoConfig(topologyContext, topologyConstructor, name, uri, database)
    this.addSinkConfiguration(c)
    return c
}

fun Stream.mongoConfig(name: String, uri: String, database: String): MongoConfig {
    val c = MongoConfig(topologyContext, topologyConstructor, name, uri, database)
    this.addLocalSinkConfiguration(c)
    return c
}

fun PartialStream.toMongo(collection: String, topicDefinition: String, config: MongoConfig) {
    val topic = Topic.fromQualified(topicDefinition, topologyContext)
    config.sinkInstancePair.add(collection to topic)
    val sinkName = config.name //
    if (sinkName.startsWith("@")) {
        throw IllegalArgumentException("Should not start a database name with @, please use a fully qualified name")
    }
    // val sinkName = ProcessorName.from(config.name)
    val sink = SinkTransformer(
        Optional.of(sinkName),
        topic,
        Optional.empty(),
        Topic.FloodplainKeyFormat.CONNECT_KEY_JSON,
        Topic.FloodplainBodyFormat.CONNECT_JSON
    )
    val transform = Transformer(rootTopology, sink, topologyContext)
    addTransformer(transform)
}
