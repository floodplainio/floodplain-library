package io.floodplain.kotlindsl

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.streams.api.TopologyContext
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

class ElasticSearchSinkConfig(val name: String, val uri: String) : Config {
    override fun materializeConnectorConfig(topologyContext: TopologyContext): Pair<String, Map<String, String>> {
        TODO("Not yet implemented")
    }

    override fun sourceElements(): List<SourceTopic> {
        TODO("Not yet implemented")
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
        TODO("Not yet implemented")
    }

    fun sink(index: String, topic: String): FloodplainSink {
        val sinkConfig = mapOf(
            "connector.class" to "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
            "connection.url" to uri,
            "tasks.max" to "1",
            "type.name" to "_doc",
            "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
            "topics" to topic,
            "schema.ignore" to "true",
            "type.name" to "_doc")
        val conn = ElasticsearchSinkConnector()
        conn.start(sinkConfig)
        val task = conn.taskClass().getDeclaredConstructor().newInstance() as SinkTask
        return ElasticSearchSink(topic, task)
    }
}

private class ElasticSearchSink(private val topic: String, private val task: SinkTask) : FloodplainSink {
    private val offsetCounter = AtomicLong(0)
    override fun send(docs: List<Pair<String, IMessage>>) {
        val list = docs.map { (key, value) ->
            val rf = value.data()
            SinkRecord(topic, 0, null, key, null, rf, offsetCounter.incrementAndGet())
        }.toList()
        task.put(list)
    }

    override fun close() {
    }
}

fun sink(uri: String, topic: String): SinkTask {

    val sinkConfig = mapOf(
        "connector.class" to "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "connection.url" to uri,
        "tasks.max" to "1",
        "type.name" to "_doc",
        "key.converter" to "org.apache.kafka.connect.storage.StringConverter",
        "topics" to topic,
        "schema.ignore" to "true",
        "type.name" to "_doc")
    val conn = ElasticsearchSinkConnector()
    conn.start(sinkConfig)
    val task = conn.taskClass().getDeclaredConstructor().newInstance() as SinkTask
    task.start(conn.taskConfigs(1).first())
    return task
}
