package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.streams.api.TopologyContext
import java.net.URL
import java.util.*

val topologyContext = TopologyContext(Optional.of("tenant"), "test", "instance", "generation")
private val logger = mu.KotlinLogging.logger {}

fun filterTest(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "mongodump")
        postgresSource("public", "actor", postgresConfig) {
            filter { msg, _ ->
                msg["last_name"] as String > "b"
            }
            mongoSink("filtercollection", "filtertopic", mongoConfig)
        }

    }
}

fun main() {
    filmWithActorList("gen_aat")
}

fun films(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("public", "film", postgresConfig) {
            each { iMessage, iMessage2 -> logger.info { "message: ${iMessage}" } }
            mongoSink("filmwithcategories", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "kafka:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun filmActorSubGroupList(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("public", "film_actor", postgresConfig) {
            group { msg -> "${msg["film_id"]}" }
            mongoSink("filmwithcategories", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "kafka:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun filmWithActorList(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("public", "film", postgresConfig) {
            joinWith(optional = true, multiple = true) {
                postgresSource("public", "film_actor", postgresConfig) {
                    joinRemote({ msg -> "${msg["actor_id"]}" }, false) {
                        postgresSource("public", "actor", postgresConfig) {
                        }
                    }
                    set { msg, state ->
                        msg["actor"] = state
                        msg
                    }
                    group { msg -> "${msg["film_id"]}" }
                }
            }
            each { iMessage, iMessage2 -> logger.info { "film: ${iMessage} actor: ${iMessage2}" } }
            set { msg, state ->
                msg["actors"] = state["list"] ?: emptyList<IMessage>()
//                val special = msg.get("special_features") as Array<String>
//                for (s in special) {
//                    logger.info("FEAT: ${s}")
//                }
//                logger.info("SPECIAL_FEATURE: ${msg.get("special_features")}")
                msg
            }
            mongoSink("filmwithcategories", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "kafka:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun joinFilms(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("public", "film", postgresConfig) {
            joinGroup({ msg -> "${msg["film_id"]}" }, false) {
                postgresSource("public", "film_category", postgresConfig) {
                    joinRemote({ msg -> "${msg["category_id"]}" }, true) {
                        postgresSource("public", "category", postgresConfig) {}
                    }
                    set { msg, state ->
                        msg["category"] = state["name"]!!
                        msg
                    }
                }
            }
            set { msg, state ->
                msg["categories"] = state["list"]!!
                msg
            }
            mongoSink("filmwithcategories", "filmwithcat", mongoConfig)
        }

    }.renderAndStart(URL("http://localhost:8083/connectors"), "kafka:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun joinAddresses(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
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
            sink("address")
            mongoSink("address", "addresstopic", mongoConfig)
        }
        postgresSource("public", "customer", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address", state)
            }
            mongoSink("customer", "filtertopic", mongoConfig)
        }
        postgresSource("public", "staff", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address", state)
            }
            mongoSink("staff", "stafftopic", mongoConfig)
        }

    }.renderAndStart(URL("http://localhost:8083/connectors"), "kafka:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}