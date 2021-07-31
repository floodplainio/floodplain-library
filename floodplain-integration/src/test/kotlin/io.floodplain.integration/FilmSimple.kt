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

import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.from
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.kotlindsl.to
import io.floodplain.mongodb.remoteMongoConfig
import io.floodplain.mongodb.toMongo
import io.floodplain.mongodb.waitForMongoDbCondition
import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.useIntegraton
import kotlinx.coroutines.cancel
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.TestInstance

private val logger = mu.KotlinLogging.logger {}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FilmSimple {

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)
    private val mongoContainer = InstantiatedContainer("mongo:latest", 27017)

    @AfterAll
    fun shutdown() {
        postgresContainer.close()
        mongoContainer.close()
    }

    /**
     * Test the simplest imaginable pipe: One source and one sink.
     */
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
                "@mongodump"
            )
            postgresSource("film", postgresConfig) {
                // Clear the last_update field, it makes no sense in a denormalized situation
                each { _, msg, _ ->
                    logger.info("Last update: ${msg["last_update"]?.javaClass}")
                }
                toMongo("filmwithactors", "$generation-filmwithcat", mongoConfig)
            }
        }.renderAndExecute {
            val database = topologyContext().topicName("@mongodump")
            flushSinks()
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                database
            ) {
                currentDatabase ->
                val collection = currentDatabase.getCollection("filmwithactors")
                collection.find().first()?.let {
                    logger.info("Example doc: $it")
                }
                val countDocuments = collection.countDocuments()
                if (countDocuments == 1000L) {
                    1000L
                } else {
                    null
                }
            }
            assertNotNull(hits)
            connectJobs().forEach { it.cancel("ciao!") }
        }
    }

    /**
     * Test the simplest imaginable pipe: One source and one sink.
     */
    @Test
    fun testPostgresSourceFromArguments() {
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
                toMongo("filmwithactors", "$generation-filmwithcat", mongoConfig)
            }
        }.runWithArguments { topologyContext ->
            // val database = topologyContext.topicName("@mongodump")
            // flushSinks()
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "${topologyContext.generation}-mongodump"
            ) { currentDatabase ->
                val collection = currentDatabase.getCollection("filmwithactors")
                val countDocuments = collection.countDocuments()
                if (countDocuments == 1000L) {
                    1000L
                } else {
                    null
                }
            }
            assertNotNull(hits)
        }
    }

    @Test
    fun testPostgresWithExtraTopic() {
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
                to("$generation-intermediatesink")
            }
            from("$generation-intermediatesink") {
                toMongo("filmwithactors", "$generation-filmwithcat", mongoConfig)
            }
        }.renderAndExecute {
            flushSinks()
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "${topologyContext.generation}-mongodump"
            ) { mongoDatabase ->
                val collection = mongoDatabase.getCollection("filmwithactors")
                val countDocuments = collection.countDocuments()
                if (countDocuments == 1000L) {
                    1000L
                } else {
                    null
                }
            } as Long?
            assertNotNull(hits)
            connectJobs().forEach { it.cancel("ciao!") }
        }
    }
}
