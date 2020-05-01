package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.mongoConfig
import io.floodplain.kotlindsl.mongoSink
import io.floodplain.kotlindsl.pipe
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import java.net.URL
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

fun main() {
    joinFilms("generation1")
}

fun joinFilms(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("public", "film", postgresConfig) {
            joinGrouped(false) {
                postgresSource("public", "film_category", postgresConfig) {
                    joinRemote({ msg -> "${msg["category_id"]}" }, true) {
                        postgresSource("public", "category", postgresConfig) {}
                    }
                    set { _,msg, state ->
                        msg["category"] = state["name"] ?: "unknown"
                        msg
                    }
                    group { msg -> "${msg["film_id"]}" }
                }
            }
            set { _,msg, state ->
                msg["categories"] = state["list"] ?: empty()
                msg
            }
            mongoSink("filmwithcategories", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}
