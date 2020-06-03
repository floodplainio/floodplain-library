package io.floodplain.kotlindsl

import io.floodplain.kotlindsl.message.empty
import org.junit.Test

class TestElasticSearch {
    @Test
    fun testElasticInsert() {
        val config = ElasticSearchSinkConfig("elasticName", "http://localhost:9200")
        val sink = config.sink("someindex", "mytopic")
        sink.send(listOf("id1" to empty().set("body", "I am a fluffy rabbit and I have fluffy feet")))
        sink.close()
    }
}
