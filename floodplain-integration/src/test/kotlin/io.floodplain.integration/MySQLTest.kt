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
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.join
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.jdbc.mysqlSource
import io.floodplain.jdbc.mysqlSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.remoteMongoConfig
import io.floodplain.mongodb.toMongo
import io.floodplain.mongodb.waitForMongoDbCondition
import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.useIntegraton
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

private val logger = mu.KotlinLogging.logger {}

@Suppress("UNCHECKED_CAST")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MySQLTest {

    private fun createMySql(): InstantiatedContainer {
        return InstantiatedContainer(
            "debezium/example-mysql:2.1.1.Final",
            3306,
            mapOf(
                "MYSQL_ROOT_PASSWORD" to "mysecretpassword",
                "MYSQL_DATABASE" to "wpdb",
                "MYSQL_USER" to "mysqluser",
                "MYSQL_PASSWORD" to "mysqlpw",
                "MYSQL_ROOT_HOST" to "%"
            )
        )
    }

    private fun createMongodb(): InstantiatedContainer {
        return InstantiatedContainer("mongo:latest", 27017)
    }

    @Test
    fun testSimple() {
        val mysqlContainer = createMySql()
        val mongoContainer = createMongodb()
        if (!useIntegraton) {
            logger.warn("Skipping integration test")
            return
        }
        stream {
            val mysqlConfig = mysqlSourceConfig(
                "mypostgres",
                mysqlContainer.host,
                mysqlContainer.exposedPort,
                "root",
                "mysecretpassword",
                "inventory",
                "mypostgres",
            )
            val mongoConfig = remoteMongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "mongodump"
            )
            mysqlSource("inventory.customers", mysqlConfig) {
                toMongo("customers", "customers", mongoConfig)
            }
        }.renderAndExecute {
            val databaseInstance = "mongodump"
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                databaseInstance
            ) { database ->
                val collection = database.getCollection("customers")
                val countDocuments = collection.countDocuments()
                logger.info("# of documents: $countDocuments")
                if (countDocuments == 4L) {
                    4L
                } else {
                    null
                }
            } as Long?
            connectJobs().forEach {
                it.cancel()
            }
            mysqlContainer.close()
            mongoContainer.close()
            assertNotNull(hits)
        }
    }

    @Test
    fun testRuntimeParamParser() {
        val mysqlContainer = createMySql()
        val mongoContainer = createMongodb()
        if (!useIntegraton) {
            logger.warn("Skipping integration test")
            return
        }
        stream {
            val mysqlConfig = mysqlSourceConfig(
                "mypostgres",
                mysqlContainer.host,
                mysqlContainer.exposedPort,
                "root",
                "mysecretpassword",
                "inventory",
                "mypostgres",
            )
            val mongoConfig = remoteMongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
            )
            mysqlSource("inventory.customers", mysqlConfig) {
                toMongo("customers", "$generation-customers", mongoConfig)
            }
        }.renderAndExecute {
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "${topologyContext.generation}-mongodump"
            ) { database ->
                val customerCount = database.getCollection("customers").countDocuments()
                val orderCount = database.getCollection("orders").countDocuments()
                val productCount = database.getCollection("products").countDocuments()
                logger.info("# of documents: $customerCount $orderCount $productCount")
                if (customerCount == 4L) {
                    4L
                } else {
                    null
                }
            } as Long?
            assertNotNull(hits)
            mysqlContainer.close()
            mongoContainer.close()
        }
    }

    @Test
    fun testInventory() {
        val mysqlContainer = createMySql()
        val mongoContainer = createMongodb()
        stream {
            val mysqlConfig = mysqlSourceConfig(
                "mypostgres",
                mysqlContainer.host,
                mysqlContainer.exposedPort,
                "root",
                "mysecretpassword",
                "inventory",
                "mypostgres",
            )
            val mongoConfig = remoteMongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "$generation-mongodump"
            )
            mysqlSource("inventory.customers", mysqlConfig) {
                each { key, customer, _ ->
                    logger.info("Key: $key message: $customer")
                }
                joinGrouped(true) {
                    mysqlSource("inventory.addresses", mysqlConfig) {
                        group { msg -> "${msg["customer_id"]}" }
                    }
                }
                set { _, customer, addresses ->
                    val addressList: List<IMessage> = addresses["list"] as List<IMessage>
                    customer["addresses"] = addressList
                    customer
                }
                each { key, customer, _ ->
                    logger.info("Result Key: $key message: $customer")
                }
                toMongo("customers", "$generation-customers", mongoConfig)
            }
            mysqlSource("inventory.products", mysqlConfig) {
                join {
                    mysqlSource("inventory.products_on_hand", mysqlConfig) {}
                }
                set { _, product, productOnHand ->
                    product["quantity"] = productOnHand.integer("quantity")
                    product
                }
                toMongo("products", "$generation-products", mongoConfig)
            }
            mysqlSource("inventory.orders", mysqlConfig) {
                toMongo("orders", "$generation-orders", mongoConfig)
            }
        }.renderAndExecute {
            // delay(1000000)
            val databaseInstance = "${topologyContext.generation}-mongodump"
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                databaseInstance,
                600000
            ) { database ->
                val customerCount = database.getCollection("customers").countDocuments()
                val orderCount = database.getCollection("orders").countDocuments()
                val productCount = database.getCollection("products").countDocuments()
                logger.info("# of documents: $customerCount")
                if (customerCount == 4L && orderCount == 4L && productCount == 9L) {
                    4L
                } else {
                    null
                }
            } as Long?
            assertNotNull(hits)
            mysqlContainer.close()
            mongoContainer.close()
        }
    }
    // can make this a proper unit test when I have a persisted wordpress installation image
    @Test @Disabled
    fun testWordpress() {
        stream {
            val mysqlConfig = mysqlSourceConfig(
                "mysqlsource",
                "localhost",
                3306,
                "root",
                "mysecretpassword",
                "wpdb",
                "topicPrefix",
            )
            val mongoConfig = remoteMongoConfig("mongosink", "mongodb://localhost", "@mongodump2")
            mysqlSource("wpdb.wp_posts", mysqlConfig) {
                each { key, msg, _ ->
                    logger.info("Detected key: $key and message: $msg")
                }
                filter { _, msg ->
                    msg["post_status"] == "publish"
                }
                toMongo("posts", "$generation-topicdef", mongoConfig)
            }
        }.renderAndExecute {
            delay(1000000)
        }
    }
}
