package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
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
//    filmWithActorList("some_generation")
//    joinFilms("mygen")
    joinAddresses("generation_52")

}

fun films(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("public", "film", postgresConfig) {
            each { iMessage, iMessage2 -> logger.info { "message: ${iMessage}" } }
            mongoSink("filmwithcategories", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
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
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}

fun filmWithActorList(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        // Start with the 'film' collection
        postgresSource("public", "film", postgresConfig) {
            // Clear the last_update field, it makes no sense in a denormalized situation
            set {
                film,_ ->film["last_update"] = null; film
            }
            // Join with something that also uses the film_id key space.
            // optional = true so films without any actors (animation?) will propagate
            // multiple = true we're not joining with something that actually 'has' a film id
            // we are joining with something that is grouped by film_id
            join(optional = true) {

                postgresSource("public", "film_actor", postgresConfig) {
                    joinRemote({ msg -> "${msg["actor_id"]}" }, false) {
                        postgresSource("public", "actor", postgresConfig) {
                        }
                    }
                    // copy the first_name, last_name and actor_id to the film_actor message, drop the last update
                    set { actor_film, actor ->
                        actor_film["last_name"] = actor["last_name"]
                        actor_film["first_name"] = actor["first_name"]
                        actor_film["actor_id"] = actor["actor_id"]
                        actor_film["last_update"]=null
                        actor_film
                    }
                    // group the film_actor stream by film_id
                    group { msg -> "${msg["film_id"]}" }
                }
            }
            // ugly hack: As lists of messages can't be toplevel, a grouped message always consist of a single, otherwise empty message, that only
            // contains one field, which is a list of the grouped messages, and that field is always named 'list'
            set { film, actorlist ->
                film["actors"] = actorlist["list"] ?: emptyList<IMessage>()
                film
            }
            // pass this message to the mongo sink
            mongoSink("filmwithcategories", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
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
                    set { msg, state ->
                        msg["category"] = state["name"]?:"unknown"
                        msg
                    }
                    group { msg-> "${msg["film_id"]}" }
                }
            }
            set { msg, state ->
                msg["categories"] = state["list"]?: empty()
                msg
            }
            mongoSink("filmwithcategories", "filmwithcat", mongoConfig)
        }

    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
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
        postgresSource("public", "staff", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("address") {}
            }
            set { msg, state ->
                msg.set("address", state)
            }
            mongoSink("store", "storetopic", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}