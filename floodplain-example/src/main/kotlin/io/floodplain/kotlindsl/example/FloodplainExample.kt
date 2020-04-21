package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.empty
import java.net.URL
import java.util.*

private val logger = mu.KotlinLogging.logger {}

// Scan has issues, TODO

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
                            set { msg, state ->
                                msg.set("country", state)
                            }
                        }
                    }
                    set { msg, state ->
                        msg.set("city", state)
                    }
                    sink("@address")
                },
                postgresSource("public", "customer", postgresConfig) {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("address") {}
                    }
                    set { msg, state ->
                        msg.set("address", state)
                    }
                    join {
                        source("@consumertotals") {}
                    }
                    set { customer, totals ->
                        customer["total"] = totals["total"]; customer
                    }
                    mongoSink("customer", "@customer", mongoConfig)
                },
                postgresSource("public", "store", postgresConfig) {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("address") {}
                    }
                    set { msg, state ->
                        msg.set("address", state)
                    }
                    mongoSink("store", "@store", mongoConfig)
                },
                postgresSource("public", "staff", postgresConfig) {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("address") {}
                    }
                    set { msg, state ->
                        msg.set("address", state)
                    }
                    mongoSink("staff", "@staff", mongoConfig)
                },
                postgresSource("public", "payment", postgresConfig) {
                    scan({ msg -> msg["customer_id"].toString() }, { msg -> empty().set("total", 0.0).set("customer_id", msg["customer_id"]) },
                            {
                                set { msg, state ->
                                    state["total"] = state["total"] as Double + msg["amount"] as Double;
                                    state["customer_id"] = msg["customer_id"]!!;
                                    state
                                }
                            },
                            {
                                set { msg, state -> state["total"] = state["total"] as Double - msg["amount"] as Double; state }
                            }
                    )
                    set { customer, totals ->
                        customer["total"] = totals["total"]; customer
                    }
                    sink("@customertotals")
                })
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
}