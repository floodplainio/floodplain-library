package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.empty
import java.net.URL
import java.util.*

private val logger = mu.KotlinLogging.logger {}

// Scan has issues, TODO

fun main() {
    val generation = "mygeneration"

    pipe(generation) {

        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "mongodump")
        postgresSource("public", "customer", postgresConfig) {
            join {
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
                }
            }
            set { msg, state ->
                msg["payments"] = state; msg
            }
            mongoSink("justtotal", "myfinaltopic", mongoConfig)

        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
}