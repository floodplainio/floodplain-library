package io.floodplain.kotlindsl.example

import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.IMessage
import java.net.URL
import java.util.*

val topologyContext = TopologyContext(Optional.of("tenant"), "test", "instance", "generation")
private val logger = mu.KotlinLogging.logger {}

fun filterTest() {

    pipe(topologyContext, TopologyConstructor()) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "mongodump")
        debeziumSource("public", "actor", postgresConfig) {
            filter { msg, _ ->
                msg["last_name"] as String > "b"
            }
            mongoSink(topologyContext, "filtercollection", "filtertopic", mongoConfig)
        }

    }
}

fun main() {
//    joinAddresses("gen_zzzz")
    filmWithActorList("gen_aat")
}

fun films(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        debeziumSource("public", "film", postgresConfig) {
            each { iMessage, iMessage2 -> logger.info { "message: ${iMessage}" } }
            mongoSink(topologyContext,"filmwithcategories","filmwithcat",mongoConfig)
        }
    }.renderAndStart(URL( "http://localhost:8083/connectors"),"kafka:9092",UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun filmActorSubGroupList(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        debeziumSource("public", "film_actor", postgresConfig) {
            group { msg -> "${msg["film_id"]}" }
            mongoSink(topologyContext, "filmwithcategories", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL( "http://localhost:8083/connectors"),"kafka:9092",UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun filmWithActorList(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        debeziumSource("public", "film", postgresConfig) {
            joinWith(optional = true, multiple = true) {
                debeziumSource("public", "film_actor", postgresConfig) {
                    joinRemote({msg->"${msg["actor_id"]}"}, false) {
                        debeziumSource("public", "actor", postgresConfig) {
                        }
                    }
                    set { msg,state->msg["actor"]=state
                        msg
                    }
                    group  { msg -> "${msg["film_id"]}"  }
                }
            }
            each { iMessage, iMessage2 -> logger.info { "film: ${iMessage} actor: ${iMessage2}" } }
            set {
                msg,state->msg["actors"]=state["list"]?: emptyList<IMessage>()
//                val special = msg.get("special_features") as Array<String>
//                for (s in special) {
//                    logger.info("FEAT: ${s}")
//                }
//                logger.info("SPECIAL_FEATURE: ${msg.get("special_features")}")
                msg
            }
            mongoSink(topologyContext,"filmwithcategories","filmwithcat",mongoConfig)
        }
    }.renderAndStart(URL( "http://localhost:8083/connectors"),"kafka:9092",UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun joinFilms(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        debeziumSource("public", "film", postgresConfig) {
            joinGroup({ msg -> "${msg["film_id"]}" },false) {
                debeziumSource("public", "film_category", postgresConfig) {
                    joinRemote({ msg -> "${msg["category_id"]}" },true) {
                        debeziumSource("public", "category", postgresConfig) {}
                    }
                    set {
                        msg,state->msg["category"]=state["name"]!!
                        msg
                    }
                }
            }
            set {
                msg,state->msg["categories"]=state["list"]!!
                msg
            }
            mongoSink(topologyContext,"filmwithcategories","filmwithcat",mongoConfig)

        }

    }.renderAndStart(URL( "http://localhost:8083/connectors"),"kafka:9092",UUID.randomUUID().toString())
    logger.info { "done!" }
}
fun joinAddresses(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        debeziumSource("public", "address", postgresConfig) {
            joinRemote({ msg -> "${msg["city_id"]}" },false) {
                debeziumSource("public", "city", postgresConfig) {
                    joinRemote({ msg -> "${msg["country_id"]}" },false) {
                        debeziumSource("public", "country", postgresConfig) {}
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
            mongoSink(topologyContext, "address", "addresstopic", mongoConfig)
        }
        debeziumSource("public", "customer", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" },false) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address",state)
            }
            mongoSink(topologyContext, "customer", "filtertopic", mongoConfig)
        }
        debeziumSource("public", "staff", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" },false) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address",state)
            }
            mongoSink(topologyContext, "staff", "stafftopic", mongoConfig)
        }

    }.renderAndStart(URL( "http://localhost:8083/connectors"),"kafka:9092",UUID.randomUUID().toString())
    logger.info { "done!" }
}