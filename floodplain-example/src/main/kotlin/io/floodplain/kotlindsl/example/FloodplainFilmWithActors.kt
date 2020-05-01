package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.IMessage
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
    filmWithActorList("generation2")
}

fun filmWithActorList(generation: String) {
    pipe(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        // Start with the 'film' collection
        postgresSource("public", "film", postgresConfig) {
            // Clear the last_update field, it makes no sense in a denormalized situation
            set { _,film, _ ->
                film["last_update"] = null; film
            }
            // Join with something that also uses the film_id key space.
            // optional = true so films without any actors (animation?) will propagate
            // multiple = true we're not joining with something that actually 'has' a film id
            // we are joining with something that is grouped by film_id
            joinGrouped(optional = true) {

                postgresSource("public", "film_actor", postgresConfig) {
                    joinRemote({ msg -> "${msg["actor_id"]}" }, false) {
                        postgresSource("public", "actor", postgresConfig) {
                        }
                    }
                    // copy the first_name, last_name and actor_id to the film_actor message, drop the last update
                    set { _,actor_film, actor ->
                        actor_film["last_name"] = actor["last_name"]
                        actor_film["first_name"] = actor["first_name"]
                        actor_film["actor_id"] = actor["actor_id"]
                        actor_film["last_update"] = null
                        actor_film
                    }
                    // group the film_actor stream by film_id
                    group { msg -> "${msg["film_id"]}" }
                }
            }
            // ugly hack: As lists of messages can't be toplevel, a grouped message always consist of a single, otherwise empty message, that only
            // contains one field, which is a list of the grouped messages, and that field is always named 'list'
            // Ideas welcome
            set { _,film, actorlist ->
                film["actors"] = actorlist["list"] ?: emptyList<IMessage>()
                film
            }
            // pass this message to the mongo sink
            mongoSink("filmwithactors", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
    logger.info { "done!" }
}
