package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.mongoConfig
import io.floodplain.kotlindsl.mongoSink
import io.floodplain.kotlindsl.pipe
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import java.net.URL
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

fun main() = pipe {
    val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
    val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
    postgresSource("public", "film", pgConfig) {
        each {
            key,message,sec -> logger.info ("Key: $key")
        }
        mongoSink("justfilm", "justfilm", mongoConfig)
    }
}.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())

fun main2() = pipe {
    val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
    val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
    postgresSource("public", "film", pgConfig) {
        joinRemote({ msg -> msg["language_id"].toString() }) { postgresSource("public", "language", pgConfig) {} }
        set { _, film, language ->
            film["language"] = language["name"]; film
        }
        mongoSink("filmwithlanguage", "filmwithlanguage", mongoConfig)
    }
}.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())

fun mainold() {
    pipe("mygeneration") {
        val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
        postgresSource("public", "actor", pgConfig) {
            set { _, msg, _ -> msg["last_update"] = null; msg }
            filter { _, msg -> (msg["actor_id"] as Int) < 10 }
            mongoSink("mycollection", "sometopic", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
}
