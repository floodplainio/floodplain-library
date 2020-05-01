package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.mongoConfig
import io.floodplain.kotlindsl.mongoSink
import io.floodplain.kotlindsl.pipe
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import java.net.URL
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

fun filter(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "mongodump")
        postgresSource("public", "actor", postgresConfig) {
            filter { _, msg ->
                (msg["last_name"] as String).startsWith("G", true)
            }
            mongoSink("filtercollection", "filtertopic", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun main() {
    filter("generation_52")
}
