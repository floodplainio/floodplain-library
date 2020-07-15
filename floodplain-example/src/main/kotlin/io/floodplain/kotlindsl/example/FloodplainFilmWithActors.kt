/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun main() {
    filmWithActorList("generation3")
}

fun filmWithActorList(generation: String) {
    stream(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental", "public")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        // Start with the 'film' collection
        postgresSource("film", postgresConfig) {
            // Clear the last_update field, it makes no sense in a denormalized situation
            set { _, film, _ ->
                film["last_update"] = null; film
            }
            // Join with something that also uses the film_id key space.
            // optional = true so films without any actors (animation?) will propagate
            // multiple = true we're not joining with something that actually 'has' a film id
            // we are joining with something that is grouped by film_id
            joinGrouped(optional = true) {

                postgresSource("film_actor", postgresConfig) {
                    joinRemote({ msg -> "${msg["actor_id"]}" }, false) {
                        postgresSource("actor", postgresConfig) {
                        }
                    }
                    // copy the first_name, last_name and actor_id to the film_actor message, drop the last update
                    set { _, actor_film, actor ->
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
            set { _, film, actorlist ->
                film["actors"] = actorlist["list"] ?: emptyList<IMessage>()
                film
            }
            // pass this message to the mongo sink
            mongoSink("filmwithactors", "@filmwithcat", mongoConfig)
        }
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "localhost:9092")
    logger.info { "done!" }
}
