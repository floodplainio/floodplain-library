package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.empty
import java.net.URL
import java.util.*

private val logger = mu.KotlinLogging.logger {}


fun main() {
    val myPipe = pipe("gen_6") {

        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "mongodump")
        postgresSource("public", "payment", postgresConfig) {

            scan({ msg -> msg["customer_id"].toString() }, { empty().set("total", 0.0).set("customer_id", 0) },
                    {
                        each { msg, state ->
                            logger.info { "inmsg: ${msg.toString()}" }
                            logger.info { "instate: ${state.toString()}" }
                        }
                        set { msg, state ->
                            state["total"] = state["total"] as Double + msg["amount"] as Double; state["customer_id"] = msg["customer_id"]!!; state
                        }
                    },
                    {
                        set { msg, state -> state["total"] = state["total"] as Double - msg["amount"] as Double; state }
                    }
            )

            each { msg, state ->
                logger.info { "msg: ${msg.toString()}" }
            }
            mongoSink("coll", "something", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "kafka:9092", UUID.randomUUID().toString())

}