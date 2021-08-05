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

import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.jdbc.postgresSource
import io.floodplain.jdbc.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.remoteMongoConfig
import io.floodplain.mongodb.toMongo
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun main() {
    joinFilms()
}

fun joinFilms() {
    stream {
        val postgresConfig = postgresSourceConfig(
            "mypostgres",
            "postgres",
            5432,
            "postgres",
            "mysecretpassword",
            "dvdrental",
            "public"
        )
        val mongoConfig = remoteMongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("film", postgresConfig) {
            joinGrouped {
                postgresSource("film_category", postgresConfig) {
                    joinRemote({ msg -> "${msg["category_id"]}" }, true) {
                        postgresSource("category", postgresConfig) {}
                    }
                    set { _, msg, state ->
                        msg["category"] = state["name"] ?: "unknown"
                        msg["last_update"] = null
                        msg
                    }
                    group { msg -> "${msg["film_id"]}" }
                }
            }
            set { _, msg, state ->
                msg["categories"] = state["list"] ?: empty()
                msg["last_update"] = null
                msg
            }
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
            set { _, film, actorlist ->
                film["actors"] = actorlist["list"] ?: emptyList<IMessage>()
                film
            }
            each { _, msg, _ ->
                logger.info("Message: $msg")
            }
            toMongo("filmwithcategories", "$tenant-$deployment-$generation-filmwithcat", mongoConfig)
        }
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "localhost:9092", true)
    logger.info { "done!" }
}
