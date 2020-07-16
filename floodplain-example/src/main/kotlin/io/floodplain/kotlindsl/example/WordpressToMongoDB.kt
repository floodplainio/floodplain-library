package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.mysqlSource
import io.floodplain.kotlindsl.mysqlSourceConfig
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.net.URL

private val logger = mu.KotlinLogging.logger {}
fun main() {
    stream("gen2") {
        val mysqlConfig = mysqlSourceConfig("mysqlsource", "mysql", 3306, "root", "mysecretpassword", "wpdb")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodumpalt")
        mysqlSource("wpdb.wp_posts", mysqlConfig) {
            each { key, msg, other ->
                logger.info("Detected key: $key and message: $msg")
            }
            mongoSink("posts", "@topicdef", mongoConfig)
        }
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "localhost:9092", true)
}
