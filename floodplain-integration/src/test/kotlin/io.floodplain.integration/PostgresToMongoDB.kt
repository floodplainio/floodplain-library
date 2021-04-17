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
import io.floodplain.kotlindsl.from
import io.floodplain.kotlindsl.join
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.scan
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.to
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.remoteMongoConfig
import io.floodplain.mongodb.toMongo
import io.floodplain.mongodb.waitForMongoDbCondition
import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.useIntegraton
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Test
import java.math.BigDecimal
import java.util.Date
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

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
    fun testPostgresSourceSimpler() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests; doesn't seem to work in circleci")
            return
        }
        stream("sometenant") {
            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                postgresContainer.host,
                postgresContainer.exposedPort,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )

            // val logConfig = logSinkConfig("any")
            val mongoConfig = remoteMongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
            )
            postgresSource("address", postgresConfig) {
                joinRemote({ msg -> "${msg["city_id"]}" }, false) {
                    postgresSource("city", postgresConfig) {
                    }
                }
                set { _, msg, state ->
                    msg["city"] = state
                    msg
                }
                toMongo("address", "$generation-sinktopicaddress", mongoConfig)
            }
        }.renderAndExecute {
            val database = topologyContext().topicName("@mongodump")
            flushSinks()
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                database
            ) { databaseInstance ->
                val collection = databaseInstance.getCollection("address")
                val countDocuments = collection.countDocuments()
                logger.info("# of documents: $countDocuments")
                if (countDocuments == 603L) {
                    603L
                } else {
                    null
                }
            } as Long?
            assertNotNull(hits)
            connectJobs().forEach { it.cancel("ciao!") }
            logger.info("done, test succeeded")
        }
    }

    @Test
    fun testMongoDBDataType() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests; doesn't seem to work in circleci")
            return
        }
        stream("sometenant") {
            // val logConfig = logSinkConfig("any")
            val mongoConfig = remoteMongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
            )
            from("input") {
                toMongo("testdatastructure", "output", mongoConfig)
            }
        }.renderAndExecute {
            val dateInput = Date()
            val inputMessage =  empty().set("date", dateInput)
            val aa = inputMessage.toImmutable()
            println(">> $aa")
            input("input","key1", empty().set("date", dateInput))
            val database = topologyContext().topicName("@mongodump")
            flushSinks()
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                database
            ) { databaseInstance ->
                val collection = databaseInstance.getCollection("testdatastructure")
                val countDocuments = collection.countDocuments()
                logger.info("# of documents: $countDocuments")
                if (countDocuments == 1L) {
                    val dateObject = collection.find().first()?.get("date")
                    logger.info("Date: $dateObject")
                    //
                    assertEquals(dateInput.toInstant().toEpochMilli(),dateObject as Long)
                    1L
                } else {
                    null
                }
            } as Long?
            assertNotNull(hits)
            connectJobs().forEach { it.cancel("ciao!") }

            logger.info("done, test succeeded")
        }
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
            postgresSource("address", postgresConfig) {
                joinRemote({ msg -> "${msg["city_id"]}" }, false) {
                    postgresSource("city", postgresConfig) {
                        joinRemote({ msg -> "${msg["country_id"]}" }, false) {
                            postgresSource("country", postgresConfig) {}
                        }
                        set { _, msg, state ->
                            msg.set("country", state)
                        }
                    }
                }
                set { _, msg, state ->
                    msg.set("city", state)
                }
                to("$generation-address")
            }
            postgresSource("customer", postgresConfig) {
                joinRemote({ m -> "${m["address_id"]}" }, false) {
                    from("$generation-address")
                }
                set { _, msg, state ->
                    msg.set("address", state)
                }
                toMongo("customer", "$generation-customer", mongoConfig)
            }
            postgresSource("store", postgresConfig) {
                joinRemote({ m -> "${m["address_id"]}" }, false) {
                    from("$generation-address")
                }
                set { _, msg, state ->
                    msg.set("address", state)
                }
                toMongo("store", "$generation-store", mongoConfig)
            }
            postgresSource("staff", postgresConfig) {
                joinRemote({ m -> "${m["address_id"]}" }, false) {
                    from("$generation-address") {}
                }
                set { _, msg, state ->
                    msg["address"] = state
                    msg["address_id"] = null
                    msg
                }
                toMongo("staff", "$generation-staff", mongoConfig)
            }
        }.renderAndExecute {
            val database = "${this.topologyContext.generation}-mongodump"
            flushSinks()
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                database
            ) { databaseInstance ->
                val staff = databaseInstance.getCollection("staff")
                val staffCount = staff.countDocuments()
                val store = databaseInstance.getCollection("store")
                val storeCount = store.countDocuments()
                val customer = databaseInstance.getCollection("customer")
                val customerCount = customer.countDocuments()
                logger.info("# of staff: $staffCount # of stores: $storeCount customer count: $customerCount")
                if (storeCount == 2L && staffCount == 2L && customerCount == 599L) {
                    customerCount
                } else {
                    null
                }
            } as Long?
            assertNotNull(hits)
            connectJobs().forEach { it.cancel("ciao!") }
        }
    }
    @Test
    fun testSimpleReduce() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests; doesn't seem to work in circleci")
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
            postgresSource("payment", postgresConfig) {
                scan(
                    { empty().set("total", BigDecimal(0)) },
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
                toMongo("justtotal", "$generation-myfinaltopic", mongoConfig)
            }
        }.renderAndExecute {
            val database = topologyContext().topicName("@mongodump")

            withTimeout(100000) {
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
        }
    }

    @Test
    fun testPaymentPerCustomer() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests; doesn't seem to work in circleci")
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
            postgresSource("customer", postgresConfig) {
                each { _, mms, _ ->
                    logger.info("Customer: $mms")
                }
                join {
                    postgresSource("payment", postgresConfig) {
                        scan(
                            { msg -> msg["customer_id"].toString() },
                            { empty().set("total", BigDecimal(0)) },
                            {
                                set { _, msg, state ->
                                    state["total"] = (state["total"] as BigDecimal).add(msg["amount"] as BigDecimal)
                                    state
                                }
                            },
                            {
                                set { _, msg, state ->
                                    state["total"] = (state["total"] as BigDecimal).add(msg["amount"] as BigDecimal)
                                    state
                                }
                            }
                        )
                    }
                }
                set { _, customer, paymenttotal ->
                    customer["payments"] = paymenttotal["total"]; customer
                }
                toMongo("paymentpercustomer", "$generation-myfinaltopic", mongoConfig)
            }
        }.renderAndExecute {
            val items = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "${topologyContext.generation}-mongodump"
            ) { currentDatabase ->
                val collection = currentDatabase.getCollection("paymentpercustomer")
                val items = collection.countDocuments()
                items> 100
            }
            assertNotNull(items)
        }
    }
}
