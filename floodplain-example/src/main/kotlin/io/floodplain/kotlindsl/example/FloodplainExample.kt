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

                    scan({ msg -> msg["customer_id"].toString() }, { empty().set("total", 0.0).set("customer_id", 0) },
                            {
                                each { msg, state,_ ->
                                    logger.info { "inmsg: ${msg.toString()}" }
                                    logger.info { "instate: ${state.toString()}" }
                                }
                                set { msg, state -> state["total"] = state["total"] as Double + msg["amount"] as Double; state["customer_id"] = msg["customer_id"]!!; state }
                            },
                            { set { msg, state -> state["total"] = state["total"] as Double - msg["amount"] as Double; state } }
                    )
                    set { _, state ->
                        val result = empty()
                        result["total"] ?: state["total"]
                        result["customer_id"] = state["customer_id"] as Int
                        result
                    }
                    each { msg, state, _ ->
                        logger.info { "msg: ${msg.toString()}" }
                    }
                    set { msg, state ->
                        msg.set("total", state.get("total")!!)
                    }
                }
            }
            set { msg, state ->
                msg["payments"] = state; state
            }

            mongoSink("customerwithtotal", "myfinaltopic", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
}