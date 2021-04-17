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
package io.floodplain.integration

import com.mongodb.client.MongoClients
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.remoteMongoConfig
import io.floodplain.mongodb.toMongo
import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.useIntegraton
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.bson.Document
import org.junit.After
import org.junit.Test
import kotlin.test.assertEquals

private val logger = mu.KotlinLogging.logger {}

@kotlinx.coroutines.ExperimentalCoroutinesApi
class FilmToMongoDB {

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)
    private val mongoContainer = InstantiatedContainer("mongo:latest", 27017)

    @After
    fun shutdown() {
        postgresContainer.close()
        mongoContainer.close()
    }

    @Test
    fun testPostgresSource() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests, doesn't seem to work in circleci")
            return
        }
        stream {

            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                postgresContainer.host,
                postgresContainer.exposedPort,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val mongoConfig = remoteMongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "$generation-mongodump"
            )
            postgresSource("film", postgresConfig) {
                // Clear the last_update field, it makes no sense in a denormalized situation
                set { _, film, _ ->
                    film["last_update"] = null; film
                }
                // Join with something that also    uses the film_id key space.
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
                toMongo("filmwithactors", "$generation-filmwithcat", mongoConfig)
            }
        }.renderAndExecute {
            val database = "${topologyContext.generation}-mongodump"
            var hits = 0L
            val start = System.currentTimeMillis()
            withTimeout(200000) {
                repeat(1000) {
                    MongoClients.create("mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}")
                        .use { client ->
                            repeat(1000) {
                                val collection = client.getDatabase(database).getCollection("filmwithactors")
                                hits = collection.countDocuments(Document("actors", Document("\$ne", null)))
                                logger.info("Count of Documents: $hits in database: $database")
                                if (hits == 997L) {
                                    return@withTimeout
                                }
                                delay(1000)
                            }
                        }
                }
            }
            val diff = System.currentTimeMillis() - start
            logger.info("Elapsed: $diff millis")
            assertEquals(997L, hits)
            connectJobs().forEach { it.cancel("ciao!") }
        }
    }
}
