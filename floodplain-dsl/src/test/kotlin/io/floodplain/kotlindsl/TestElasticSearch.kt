package io.floodplain.kotlindsl

import io.floodplain.kotlindsl.message.empty
import java.util.Date
import org.junit.Test

class TestElasticSearch {
    @Test
    fun testElasticInsert() {
        stream {
            source("sometopic") {
                val config = elasticSearchConfig("elasticName", "http://localhost:9200")
                // val sink = sink("someindex", "mytopic",config)
                val sink = elasticSearchSink("mysinkname", "myindex", "mytopic", config)
                repeat(1000) {
                    println("inserting number: $it")
                    sink.send(listOf("id_$it" to empty().set("body", "I am a fluffy rabbit number $it and I have fluffy feet")
                        .set("time", Date().time)))
                    // Thread.sleep(100)
                    sink.close()
                }
            }
        }.renderAndTest {
        }

        // Thread.sleep(2000)
    }
}
