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
import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.join
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.scan
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.streams
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Ignore
import org.junit.Test

private val logger = mu.KotlinLogging.logger {}

@kotlinx.coroutines.ExperimentalCoroutinesApi
class TestCombinedMongo {

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)
    private val mongoContainer = InstantiatedContainer("mongo:latest", 27017)

    @After
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
                logger.info("Not performing integration tests; doesn't seem to work in circleci")
                return
            }
            streams("any", "myinstance") {
                val postgresConfig = postgresSourceConfig(
                    "mypostgres",
                    postgresContainer.host,
                    postgresContainer.exposedPort,
                    "postgres",
                    "mysecretpassword",
                    "dvdrental",
                    "public"
                )
                val mongoConfig = mongoConfig(
                    "mongosink",
                    "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                    "@mongodump"
                )
                listOf(
                    postgresConfig.sourceSimple("address") {
                        joinRemote({ msg -> "${msg["city_id"]}" }, false) {
                            postgresConfig.sourceSimple("city") {
                                joinRemote({ msg -> "${msg["country_id"]}" }, false) {
                                    postgresConfig.sourceSimple("country") {}
                                }
                                set { _, msg, state ->
                                    msg.set("country", state)
                                }
                            }
                        }
                        set { _, msg, state ->
                            msg.set("city", state)
                        }
                        sink("@address", false)
                        // mongoSink("address", "@address",  mongoConfig)
                    },
                    postgresConfig.sourceSimple("customer") {
                        joinRemote({ m -> "${m["address_id"]}" }, false) {
                            source("@address") {}
                        }
                        set { _, msg, state ->
                            msg.set("address", state)
                        }
                        mongoSink("customer", "@customer", mongoConfig)
                    },
                    postgresConfig.sourceSimple("store") {
                        joinRemote({ m -> "${m["address_id"]}" }, false) {
                            source("@address") {}
                        }
                        set { _, msg, state ->
                            msg.set("address", state)
                        }
                        mongoSink("store", "@store", mongoConfig)
                    },
                    postgresConfig.sourceSimple("staff") {
                        joinRemote({ m -> "${m["address_id"]}" }, false) {
                            source("@address") {}
                        }
                        set { _, msg, state ->
                            msg.set("address", state)
                            msg["address_id"] = null
                            msg
                        }
                        mongoSink("staff", "@staff", mongoConfig)
                    })
            }.renderAndTest {
                logger.info("Outputs: ${outputs()}")
                delay(5000)
                val database = topologyContext().topicName("@mongodump")

                connectJobs().forEach { it.cancel("ciao!") }
                var hits = 0L
                withTimeout(200000) {
                    repeat(1000) {
                        MongoClients.create("mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}")
                            .use { client ->
                                val collection = client.getDatabase(database).getCollection("customer")
                                hits = collection.countDocuments()
                                logger.info("Count of Documents: $hits in database: $database")
                                if (hits >= 598) {
                                    return@withTimeout
                                }
                            }
                        delay(1000)
                    }
                }
                assertEquals(599, hits)
                logger.info("done, test succeeded")
            }
        }

    @Test
    fun testSimpleReduce() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests; doesn't seem to work in circleci")
            return
        }
        streams {
            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                postgresContainer.host,
                postgresContainer.exposedPort,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
            )
            listOf(postgresConfig.sourceSimple("payment") {
                scan({ empty().set("total", BigDecimal(0)) },
                    {
                        set { _, msg, state ->
                            state["total"] = (state["total"] as BigDecimal).add(msg["amount"] as BigDecimal)
                            state
                        }
                    },
                    {
                        set { _, msg, state ->
                            state["total"] = (state["total"] as BigDecimal).subtract(msg["amount"] as BigDecimal)
                            state
                        }
                    }
                )
                mongoSink("justtotal", "@myfinaltopic", mongoConfig)
            })
        }.renderAndTest {
            val database = topologyContext().topicName("@mongodump")

            withTimeout(200000) {
                repeat(100) {
                    MongoClients.create("mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}")
                        .use { client ->
                            val collection = client.getDatabase(database).getCollection("justtotal")
                            val total = collection.find().first()?.get("total") as Double? ?: 0.0
                            if (total> 61312) {
                                return@withTimeout
                            }
                            logger.info("Current total: $total")
                        }
                    delay(1000)
                }
            }
            logger.info("Test done, total computed")
            // TODO implement testing code
        }
    }

    // TODO Fix
    @Test @Ignore
    fun testPaymentPerCustomer() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests; doesn't seem to work in circleci")
            return
        }
        streams {
            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                postgresContainer.host,
                postgresContainer.exposedPort,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
            )
            listOf(
                postgresConfig.source("customer", "public") {
                    each { _, mms, _ ->
                        logger.info("Customer: $mms")
                    }
                join {
                    postgresConfig.source("payment", "public") {
                        each { _, mms, _ ->
                            logger.info("Payment: $mms")
                        }
                        scan({ msg -> msg["customer_id"].toString() }, { _ -> empty().set("total", BigDecimal(0)) },
                            {
                                set { _, msg, state ->
                                    state["total"] = (state["total"] as BigDecimal).add(msg["amount"] as BigDecimal)
                                    state
                                }
                            },
                            {
                                set { _, msg, state -> state["total"] = (state["total"] as BigDecimal).add(msg["amount"] as BigDecimal)
                                    ; state }
                            }
                        )
                    }
                }
                set { _, customer, paymenttotal ->
                    customer["payments"] = paymenttotal["total"]; customer
                }
                mongoSink("paymentpercustomer", "myfinaltopic", mongoConfig) })
        }.renderAndTest {
            val database = topologyContext().topicName("@mongodump")
            withTimeout(200000) {
                repeat(100) {
                    MongoClients.create("mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}")
                        .use { client ->
                            val collection = client.getDatabase(database).getCollection("paymentpercustomer")
                            val items = collection.countDocuments()
                            // TODO improve
                            if (items> 100) {
                                return@withTimeout
                            }
                            logger.info("Current total: $items")
                        }
                    delay(1000)
                }
            }
            logger.info("Test done, total computed")
            // TODO implement testing code
            delay(200000)
        }
    }
}
