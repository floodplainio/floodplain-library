package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.join
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.mongoConfig
import io.floodplain.kotlindsl.mongoSink
import io.floodplain.kotlindsl.pipes
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.scan
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import java.net.URL
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

fun main() {
    val generation = "mygeneration"

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
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                    }
                    join(optional = true) {
                        source("@customertotals") {}
                    }
                    set { _, customer, totals ->
                        customer["total"] = totals["total"]; customer
                    }
                    mongoSink("customer", "customer", mongoConfig)
                },
                postgresSource("public", "store", postgresConfig) {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                    }
                    mongoSink("store", "store", mongoConfig)
                },
                postgresSource("public", "staff", postgresConfig) {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                    }
                    mongoSink("staff", "staff", mongoConfig)
                },
                postgresSource("public", "payment", postgresConfig) {
                    scan({ msg -> msg["customer_id"].toString() }, { msg -> empty().set("total", 0.0).set("customer_id", msg["customer_id"]) },
                            {
                                set { _, msg, state ->
                                    state["total"] = state["total"] as Double + msg["amount"] as Double
                                    state["customer_id"] = msg["customer_id"]!!
                                    state
                                }
                            },
                            {
                                set { _, msg, state -> state["total"] = state["total"] as Double - msg["amount"] as Double; state }
                            }
                    )
                    set { _, customer, totals ->
                        customer["total"] = totals["total"]; customer
                    }
                    sink("@customertotals")
                })
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
}
