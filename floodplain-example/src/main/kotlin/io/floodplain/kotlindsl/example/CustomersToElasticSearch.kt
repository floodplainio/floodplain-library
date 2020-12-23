package io.floodplain.kotlindsl.example

import io.floodplain.elasticsearch.elasticSearchConfig
import io.floodplain.elasticsearch.elasticSearchSink
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import kotlinx.coroutines.delay

private val logger = mu.KotlinLogging.logger {}

fun main() {
    stream {
        val postgresConfig =
            postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental", "public")
        val elasticConfig = elasticSearchConfig(
            "elastic",
            "http://localhost:9200"
        )
        postgresSource("customer", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                postgresSource("address", postgresConfig) {
                    joinRemote({ msg -> "${msg["city_id"]}" }, false) {
                        postgresSource("city", postgresConfig) {
                            joinRemote({ msg -> "${msg["country_id"]}" }, false) {
                                postgresSource("country", postgresConfig) {}
                            }
                            set { _, msg, state ->
                                msg.set("country", state["country"])
                            }
                        }
                    }
                    set { _, msg, state ->
                        msg.set("city", state["city"])
                        msg.set("country", state["country"])
                    }
                }
            }
            set { _, msg, state ->
                msg.set("address", state)
                logger.info { "Customer: \n$msg" }
                msg
            }
            elasticSearchSink("elastic", "@customer", elasticConfig)
        }
    }.renderAndExecute {
        delay(Long.MAX_VALUE)
    }
}
