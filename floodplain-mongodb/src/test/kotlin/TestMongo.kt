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
import com.mongodb.client.MongoClients
import io.floodplain.kotlindsl.Source
import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import io.floodplain.kotlindsl.streams
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.util.Date
import kotlin.test.assertEquals
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.bson.Document
import org.junit.Test
import org.testcontainers.containers.GenericContainer

private val logger = mu.KotlinLogging.logger {}

class TestMongo {

    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
    var address: String? = "localhost"
    var port: Int? = 0

    var container: GenericContainer<*>? = null

    init {

        // var envVar = System.getenv("EMBED_CONTAINER")
            container = KGenericContainer("mongo:latest")
                .apply { withExposedPorts(27017) }
            container?.start()
            address = container?.getHost()
            port = container?.getFirstMappedPort()
    }

    @Test
    fun testSink() {
        stream {
            val config = mongoConfig("mongoClient", "mongodb://$address:$port", "mongo-connect-test")

            source("sometopic") {
                each { _, msg, _ -> logger.info("message: $msg") }
                filter { _, msg -> msg.long("counter") % 2 == 0L }
                mongoSink("test-collection", "myindex", config)
            }
        }.renderAndTest {
            // delay(5000)
            MongoClients.create("mongodb://$address:$port").use { client ->
                val collection = client.getDatabase("mongo-connect-test")
                    .getCollection("test-collection")
                collection.deleteMany(Document())

                repeat(100) {
                    val trip = empty().set("body", "I am a fluffy rabbit number $it and I have fluffy feet")
                        .set("time", Date().time)
                        .set("counter", it.toLong())
                    input("sometopic", "somekey_$it", trip)
                }
                val elements = outputSize("myindex")
                flushSinks()
                withTimeout(100000) {
                    repeat(1000) {
                        var collectionCount = collection
                            .countDocuments()
                        if (collectionCount == 50L) {
                            return@withTimeout
                        }
                        logger.info("Not yet right amount: $collectionCount")
                        delay(500)
                    }
                }
                assertEquals(50L, elements)

                var doccount = collection
                    .countDocuments()
                logger.info("Count of Documents: $doccount")
                assertEquals(50L, doccount)
            }
        }
    }

    @Test
    fun testMultipleSink() {
        streams {
            val config = mongoConfig("mongoClient", "mongodb://$address:$port", "mongo-connect-test")
            listOf<Source>(
                source("sometopic") {
                    each { _, msg, _ -> logger.info("message: $msg") }
                    filter { _, msg -> msg.long("counter") % 2 == 0L }
                    mongoSink("collection1", "myindex1", config)
                },
                source("sometopic") {
                    each { _, msg, _ -> logger.info("message: $msg") }
                    filter { _, msg -> msg.long("counter") % 2 == 1L }
                    mongoSink("collection2", "myindex2", config)
                }
            )
        }.renderAndTest {
            // delay(5000)
            MongoClients.create("mongodb://$address:$port").use { client ->
                val database = client.getDatabase("mongo-connect-test")
                    val collection1 = database.getCollection("collection1")
                val collection2 = database.getCollection("collection2")
                collection1.deleteMany(Document())
                collection2.deleteMany(Document())

                repeat(100) {
                    val trip = empty().set("body", "I am a fluffy rabbit number $it and I have fluffy feet")
                        .set("time", Date().time)
                        .set("counter", it.toLong())
                    input("sometopic", "somekey_$it", trip)
                }
                val elements1 = outputSize("myindex1")
                val elements2 = outputSize("myindex2")
                logger.info("Elements: $elements1 and $elements2")
                flushSinks()
                assertEquals(50L, elements1)
                assertEquals(50L, elements2)
                withTimeout(100000) {
                    repeat(1000) {
                        var col1count = collection1
                            .countDocuments()
                        var col2count = collection2
                            .countDocuments()
                        if (col1count == 50L && col2count == 50L) {
                            return@withTimeout
                        }
                        logger.info("Not yet right amount: $col1count and $col2count")
                        delay(500L)
                    }
                }
                assertEquals(50L, collection1.countDocuments())
                assertEquals(50L, collection2.countDocuments())
            }
        }
    }
}
