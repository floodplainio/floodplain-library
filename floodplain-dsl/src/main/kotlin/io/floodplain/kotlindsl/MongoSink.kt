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

import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.CoreOperators
import io.floodplain.streams.api.TopologyContext
import java.util.Optional

private val logger = mu.KotlinLogging.logger {}

class MongoConfig(val name: String, val uri: String, val database: String) : Config() {

    val sinkInstancePair: MutableList<Pair<String, String>> = mutableListOf()
    override fun materializeConnectorConfig(topologyContext: TopologyContext): Pair<String, Map<String, String>> {
        val additional = mutableMapOf<String, String>()
        sinkInstancePair.forEach { (key, value) -> additional.put("topic.override.${topologyContext.topicName(value)}.collection", key) }
        println("Pairs: $sinkInstancePair")
        val collections: String = sinkInstancePair.map { e -> e.first }.joinToString(",")
        println("Collections: $collections")
        val topics: String = sinkInstancePair.map { r -> topologyContext.topicName(r.second) }.joinToString(",")
        println("Topics: $topics")

//        topic.override.sourceB.collection=sourceB

        val generationalDatabase = topologyContext.generationalGroup(database)
        val settings = mutableMapOf("connector.class" to "com.mongodb.kafka.connect.MongoSinkConnector",
                "value.converter.schemas.enable" to "false",
                "key.converter.schemas.enable" to "false",
                "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "document.id.strategy" to "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
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

fun PartialStream.mongoSink(collection: String, topic: String, config: MongoConfig) {
    config.sinkInstancePair.add(collection to topic)
    val sink = SinkTransformer(Optional.of(config.name), topic, false, Optional.empty(), true)
    addTransformer(Transformer(sink))
}
