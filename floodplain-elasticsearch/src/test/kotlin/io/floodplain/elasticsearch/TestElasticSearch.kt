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
package io.floodplain.elasticsearch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Date
import java.util.UUID
import kotlin.test.assertEquals
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.testcontainers.containers.GenericContainer

private val logger = mu.KotlinLogging.logger {}

class TestElasticSearch {

    private val objectMapper = ObjectMapper()

    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
    var address: String? = "localhost"
    var port: Int? = 0
    var container: GenericContainer<*>? = null

    init {
        container = KGenericContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:7.7.0")
            .apply { withExposedPorts(9200)
                .withEnv("discovery.type", "single-node")
            }
        container?.start()
        address = container?.getHost()
        port = container?.getFirstMappedPort()
    }

    @Test
    fun testElasticInsert() {
        val uri = "http://$address:$port"
        stream {
            source("sometopic") {
                val config = elasticSearchConfig("elasticName", uri)
                elasticSearchSink("mysinkname", "myindex", "someindex", config)
            }
        }.renderAndTest {

            // for now TODO remove
            // address = "localhost"
            // port = 9200
            val uuid = UUID.randomUUID().toString().substring(1..7)
            repeat(1) {
                val msg = empty()
                    .set("body", "I am a fluffy rabbit number $it and I have fluffy feet")
                    .set("time", Date().time)
                    .set("uuid", uuid)
                input("sometopic", "$uuid", msg)
                logger.info("inserting number: $it and uuid: $uuid")
            }
            withTimeout(100000) {
                repeat(1000) {
                    val resultCount = queryUUIDHits(uuid)
                    if (resultCount == 1) {
                        logger.info("Found hit. continuing.")
                        return@withTimeout
                    }
                    logger.info("looping...")
                    delay(1000)
                }
            }
            assertEquals(1, queryUUIDHits(uuid))

            logger.info("deleting....")
            delete("sometopic", uuid)
            withTimeout(100000) {
                repeat(1000) {
                    val resultCount = queryUUIDHits(uuid)
                    if (resultCount == 0) {
                        logger.info("Delete processed. continuing.")
                        return@withTimeout
                    }
                }
            }
            delay(2000)
            assertEquals(0, queryUUIDHits(uuid))
        }
    }

    private fun queryUUIDHits(uuid: String): Int {
        val node = queryUUID("http://$address:$port", "q=$uuid")
        return node.get("hits").get("total").get("value").asInt()
    }

    private fun queryUUID(uri: String, query: String): JsonNode {

        val client = HttpClient.newBuilder().build()
        val request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create("$uri/_search?$query"))
            .build()
        val response: HttpResponse<String> = client.send(request, HttpResponse.BodyHandlers.ofString())
        return objectMapper.readTree(response.body())
    }
}
