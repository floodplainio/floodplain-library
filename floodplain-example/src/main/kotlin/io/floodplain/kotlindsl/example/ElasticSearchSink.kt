package io.floodplain.kotlindsl.example

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
import io.floodplain.kotlindsl.message.empty
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

class ElasticSearchSink
fun main() {
    val task = sink("http://localhost:9200", "simple.elasticsearch.data")
    task.put((1..100).map { createSinkRecord(it) }.toList())
    task.stop()
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

fun createSinkRecord(nr: Int): SinkRecord {
    val msg = empty().set("somekey", "bloep$nr").set("otherkey", "bleble").set("cre", "blabla")
    val rf = msg.data()
    return SinkRecord("simple.elasticsearch.data", 0, null, "paa$nr", null, rf, nr.toLong())
}
