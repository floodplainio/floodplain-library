package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import java.net.URL
import java.util.*

private val logger = mu.KotlinLogging.logger {}

fun main() {

    val generation = "generation1"
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("public", "address", postgresConfig) {
            joinRemote({ msg -> "${msg["city_id"]}" }, false) {
                postgresSource("public", "city", postgresConfig) {
                    joinRemote({ msg -> "${msg["country_id"]}" }, false) {
                        postgresSource("public", "country", postgresConfig) {}
                    }
                    set { msg, state ->
                        msg.set("country", state)
                    }
                }
            }
            set { msg, state ->
                msg.set("city", state)
            }
            sink("address")
        }
        postgresSource("public", "customer", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address", state)
            }
            mongoSink("customer", "filtertopic", mongoConfig)
        }
        postgresSource("public", "staff", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address", state)
            }
            mongoSink("staff", "stafftopic", mongoConfig)
        }
        postgresSource("public", "staff", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address", state)
            }
            mongoSink("store", "storetopic", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}