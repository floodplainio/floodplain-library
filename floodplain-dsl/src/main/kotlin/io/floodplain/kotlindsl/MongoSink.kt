package io.floodplain.kotlindsl

import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.CoreOperators
import io.floodplain.streams.api.TopologyContext
import java.util.*

private val logger = mu.KotlinLogging.logger {}

class MongoConfig(val name: String, val uri: String, val database: String) : Config() {

    val sinkInstancePair: MutableList<Pair<String, String>> = mutableListOf()
    override fun materializeConnectorConfig(topologyContext: TopologyContext): Pair<String, Map<String, String>> {
        val additional = mutableMapOf<String, String>()
        sinkInstancePair.forEach { (key, value) -> additional.put("topic.override.${CoreOperators.topicName(value, topologyContext)}.collection", key) }
        println("Pairs: $sinkInstancePair")
        val collections: String = sinkInstancePair.map { e -> e.first }.joinToString(",")
        println("Collections: $collections")
        val topics: String = sinkInstancePair.map { r -> CoreOperators.topicName(r.second, topologyContext) }.joinToString(",")
        println("Topics: $topics")

//        topic.override.sourceB.collection=sourceB

        val generationalDatabase = CoreOperators.generationalGroup(database, topologyContext)
        val settings = mutableMapOf("connector.class" to "com.mongodb.kafka.connect.MongoSinkConnector",
                "value.converter.schemas.enable" to "false",
                "key.converter.schemas.enable" to "false",
                "value.converter" to "ReplicationMessageConverter",
                "key.converter" to "ReplicationMessageConverter",
                "document.id.strategy" to "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
                "connection.uri" to uri,
                "database" to generationalDatabase,
                "collection" to collections,
                "topics" to topics)
        settings.putAll(additional)
        settings.forEach { (key, value) ->
            logger.info { "Setting: ${key} value: ${value}" }
        }
        return name to settings


    }
}

/**
 * Creates a config for this specific connector type, add the required params as needed. This config object will be passed
 * to all sink objects
 */
fun Pipe.mongoConfig(name: String, uri: String, database: String): MongoConfig {
    val c = MongoConfig(name, uri, database)
    this.addSinkConfiguration(c)
    return c
}


fun PartialPipe.mongoSink(collection: String, topic: String, config: MongoConfig) {
    config.sinkInstancePair.add(collection to topic)
    val sink = SinkTransformer(topic, Optional.empty())
    addTransformer(Transformer(sink))
}