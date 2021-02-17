package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.net.URL
private val logger = mu.KotlinLogging.logger {}

fun main() {

    val generation = "generation33"
    val deployment = "develop"
    val tenant = "KNBSB"

    stream(tenant, deployment, generation) {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://mongo", "@mongodump")
        source("sportlinkkernel-CALENDARDAY") {
            each { key, main, _ ->
                logger.info("Key: $key message: $main")
            }
            mongoSink("calendarday", "@sometopic", mongoConfig)
        }
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "10.8.0.7:9092")
}
