package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.mongoConfig
import io.floodplain.kotlindsl.mongoSink
import io.floodplain.kotlindsl.pipes
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun main() {

    val generation = "generation1"
    pipes(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        listOf(
        postgresSource("public", "address", postgresConfig) {
            joinRemote({ msg -> "${msg["city_id"]}" }, false) {
                postgresSource("public", "city", postgresConfig) {
                    joinRemote({ msg -> "${msg["country_id"]}" }, false) {
                        postgresSource("public", "country", postgresConfig) {}
                    }
                    set { _, msg, state ->
                        msg.set("country", state)
                    }
                }
            }
            set { _, msg, state ->
                msg.set("city", state)
            }
            sink("@address")
        },
        postgresSource("public", "customer", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { _, msg, state ->
                msg.set("address", state)
            }
            mongoSink("customer", "@customer", mongoConfig)
        },
        postgresSource("public", "store", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { _, msg, state ->
                msg.set("address", state)
            }
            mongoSink("store", "@store", mongoConfig)
        },
        postgresSource("public", "staff", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { _, msg, state ->
                msg.set("address", state)
            }
            mongoSink("staff", "@staff", mongoConfig)
        })
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", "appId")
    logger.info { "done!" }
}
