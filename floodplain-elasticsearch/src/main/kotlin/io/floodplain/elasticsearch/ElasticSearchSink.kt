package io.floodplain.elasticsearch

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
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

fun Stream.elasticSearchConfig(name: String, uri: String): ElasticSearchSinkConfig {
    val c = ElasticSearchSinkConfig(name, uri, this.context, this)
    return this.addSinkConfiguration(c) as ElasticSearchSinkConfig
}

class ElasticSearchSinkConfig(val name: String, val uri: String, val context: TopologyContext, stream: Stream) :
    Config {
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
}

private class ElasticSearchSink(private val topic: String, private val task: SinkTask) : FloodplainSink {
    private val offsetCounter = AtomicLong(System.currentTimeMillis())
    override fun send(docs: List<Triple<Topic, String, IMessage?>>) {
        val list = docs.map { (recordTopic, key, value) ->
            logger.info("Sending document to elastic. Key: $key message: $value")
            val data = value?.data()
            logger.info("Data: $data")
            SinkRecord(topic, 0, null, key, null, data, offsetCounter.incrementAndGet())
        }.toList()
        task.put(list)
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
    val sinkTransformer = SinkTransformer(Optional.of(sinkProcessorName), topic, false, Optional.empty(), false)
    addTransformer(Transformer(sinkTransformer))

    val sinkConfig = mapOf(
        "connector.class" to "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "connection.url" to config.uri,
        "tasks.max" to "1",
        "type.name" to "_doc",
        "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
        "topics" to topic.qualifiedString(config.context),
        "schema.ignore" to "true",
        "behavior.on.null.values" to "delete",
        "type.name" to "_doc")
    val conn = ElasticsearchSinkConnector()
    conn.start(sinkConfig)
    val task = conn.taskClass().getDeclaredConstructor().newInstance() as SinkTask
    task.start(sinkConfig)
    val sink = ElasticSearchSink(topic.qualifiedString(config.context), task)
    config.sinks[topic] = sink
    return sink
}
