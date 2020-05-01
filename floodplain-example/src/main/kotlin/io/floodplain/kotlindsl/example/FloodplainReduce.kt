package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.join
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.mongoConfig
import io.floodplain.kotlindsl.mongoSink
import io.floodplain.kotlindsl.pipe
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.scan
import io.floodplain.kotlindsl.set
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun main() {
    val myPipe = pipe("gen_7") {

        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "mongodump")
        postgresSource("public", "customer", postgresConfig) {
            join {
                postgresSource("public", "payment", postgresConfig) {
                    scan({ msg -> msg["customer_id"].toString() }, { msg -> empty().set("total", 0.0).set("customer_id", msg["customer_id"]) },
                            {
                                set { msg, state ->
                                    state["total"] = state["total"] as Double + msg["amount"] as Double
                                    state["customer_id"] = msg["customer_id"]!!
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
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", "reduceExample")
}
