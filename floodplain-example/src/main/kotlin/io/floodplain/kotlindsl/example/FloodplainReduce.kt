package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.empty
import java.net.URL
import java.util.*

private val logger = mu.KotlinLogging.logger {}


// Does not work well yet. TODO
fun main() {
    val myPipe = pipe("gen_6") {

        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "mongodump")
        postgresSource("public", "customer", postgresConfig) {
            each {
                total,_,key -> logger.info ("Customer found: ${total.toString()} key: $key")
            }
            join(optional = true) {
                postgresSource("public", "payment", postgresConfig) {
                    scan({ msg -> msg["customer_id"].toString() }, { msg -> empty().set("total", 0.0).set("customer_id", msg["customer_id"]) },
                            {
                                set { msg, state -> state["total"] = state["total"] as Double + msg["amount"] as Double; state["customer_id"] = msg["customer_id"]!!; state }
                            },
                            {
                                set { msg, state -> state["total"] = state["total"] as Double - msg["amount"] as Double; state }
                            }
                    )
                    each {
                        total,_,key -> logger.info ("Total found: ${total.toString()} key: $key")
                    }
                }
            }
            set {
                customer,totals->customer["total"]= totals["total"]; customer
            }
            mongoSink("coll", "something", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
}
