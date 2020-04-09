package io.floodplain.kotlindsl.example

import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import io.floodplain.kotlindsl.*
import java.net.URL
import java.util.*

val topologyContext = TopologyContext(Optional.of("tenant"), "test", "instance", "generation")
private val logger = mu.KotlinLogging.logger {}

fun filterTest() {

    pipe(topologyContext, TopologyConstructor()) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "mongodump")
        debeziumSource("public", "actor", postgresConfig) {
            filter { msg, _ ->
                msg["last_name"] as String > "b"
            }
            mongoSink(topologyContext, "filtercollection", "filtertopic", mongoConfig)
        }

    }
}

fun main() {
    pipe("gen_z") {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        debeziumSource("public", "address", postgresConfig) {
            joinRemote({ msg -> "${msg["city_id"]}" }) {
                debeziumSource("public", "city", postgresConfig) {
                    joinRemote({ msg -> "${msg["country_id"]}" }) {
                        debeziumSource("public", "country", postgresConfig) {}
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
            mongoSink(topologyContext, "address", "addresstopic", mongoConfig)
        }
        debeziumSource("public", "customer", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address",state)
            }
            mongoSink(topologyContext, "customer", "filtertopic", mongoConfig)
        }
    }.renderAndStart(URL( "http://localhost:8083/connectors"),"kafka:9092",UUID.randomUUID().toString())
    logger.info { "done!" }
}