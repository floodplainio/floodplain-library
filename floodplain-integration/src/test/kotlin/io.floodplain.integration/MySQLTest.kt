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
import io.floodplain.kotlindsl.mysqlSource
import io.floodplain.kotlindsl.mysqlSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import io.floodplain.mongodb.toMongo
import io.floodplain.mongodb.waitForMongoDbCondition
import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.useIntegraton
import kotlinx.coroutines.delay
import org.junit.After
import org.junit.Ignore
import org.junit.Test
import kotlin.test.assertNotNull

private val logger = mu.KotlinLogging.logger {}

@Suppress("UNCHECKED_CAST")
class MySQLTest {

    private val mysqlContainer = InstantiatedContainer(
        "debezium/example-mysql:1.2",
        3306,
        mapOf(
            "MYSQL_ROOT_PASSWORD" to "mysecretpassword",
            "MYSQL_DATABASE" to "wpdb",
            "MYSQL_USER" to "mysqluser",
            "MYSQL_PASSWORD" to "mysqlpw",
            "MYSQL_ROOT_HOST" to "%"
        )
    )
    private val mongoContainer = InstantiatedContainer("mongo:latest", 27017)

    @After
    fun shutdown() {
        mysqlContainer.close()
        mongoContainer.close()
    }

    @Test
    fun testSimple() {
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
                "inventory"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
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
            assertNotNull(hits)
        }
    }

    @Test
    fun testRuntimeParamParser() {
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
                "inventory"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
            )
            mysqlSource("inventory.customers", mysqlConfig) {
                mongoSink("customers", "@customers", mongoConfig)
            }
        }.runWithArguments { topologyContext ->
            val databaseInstance = topologyContext.topicName("@mongodump")
            val hits = waitForMongoDbCondition(
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                databaseInstance
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
        }
    }

    @Test
    fun testInventory() {
        stream {
            val mysqlConfig = mysqlSourceConfig(
                "mypostgres",
                mysqlContainer.host,
                mysqlContainer.exposedPort,
                "root",
                "mysecretpassword",
                "inventory"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
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
                mongoSink("customers", "@customers", mongoConfig)
            }
            mysqlSource("inventory.products", mysqlConfig) {
                join {
                    mysqlSource("inventory.products_on_hand", mysqlConfig) {}
                }
                set { _, product, product_on_hand ->
                    product["quantity"] = product_on_hand.integer("quantity")
                    product
                }
                mongoSink("products", "@products", mongoConfig)
            }
            mysqlSource("inventory.orders", mysqlConfig) {
                mongoSink("orders", "@orders", mongoConfig)
            }
        }.renderAndExecute {
            // delay(1000000)
            val databaseInstance = topologyContext().topicName("@mongodump")
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
        }
    }
    // can make this a proper unit test when I have a persisted wordpress installation image
    @Test @Ignore
    fun testWordpress() {
        stream {
            val mysqlConfig = mysqlSourceConfig(
                "mysqlsource",
                "localhost",
                3306,
                "root",
                "mysecretpassword",
                "wpdb"
            )
            val mongoConfig = mongoConfig("mongosink", "mongodb://localhost", "@mongodump2")
            mysqlSource("wpdb.wp_posts", mysqlConfig) {
                each { key, msg, _ ->
                    logger.info("Detected key: $key and message: $msg")
                }
                filter { _, msg ->
                    msg["post_status"] == "publish"
                }
                mongoSink("posts", "@topicdef", mongoConfig)
            }
        }.renderAndExecute {
            delay(1000000)
        }
    }
}
