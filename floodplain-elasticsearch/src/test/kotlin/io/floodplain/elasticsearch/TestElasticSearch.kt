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
import io.floodplain.test.InstantiatedContainer
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.NullNode
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Date
import java.util.UUID
import kotlin.test.assertEquals

private val logger = mu.KotlinLogging.logger {}

class TestElasticSearch {

    private val objectMapper = ObjectMapper()

    private val container = InstantiatedContainer(
        "docker.elastic.co/elasticsearch/elasticsearch-oss:7.7.0",
        9200,
        mapOf("discovery.type" to "single-node")
    )

    @Test
    fun testElasticInsert() {
        val uri = "http://${container.host}:${container.exposedPort}"
        stream {
            source("sometopic") {
                val config = elasticSearchConfig("elasticName", uri)
                elasticSearchSink("mysinkname", "myindex", config)
            }
        }.renderAndExecute {

            // for now TODO remove
            // address = "localhost"
            // port = 9200
            val poem =
                """It's a high pitched sound
                Hot rubber eternally pressing against a blackened pavement
                A wheel is forever
                A car is infinity times four 
                """.trimIndent()
            val uuid = UUID.randomUUID().toString().substring(1..7)
            repeat(1) {
                val msg = empty()
                    .set("body", poem)
                    .set("time", Date().time)
                    .set("uuid", uuid)
                input("sometopic", uuid, msg)
                logger.info("inserting number: $it and uuid: $uuid")
            }
            withTimeout(300000) {
                repeat(1000) {
                    val resultCount = queryUUIDHits("eternal*")
                    if (resultCount == 1) {
                        logger.info("Found hit. continuing.")
                        return@withTimeout
                    }
                    logger.info("looping...")
                    delay(2000)
                }
            }
            assertEquals(1, queryUUIDHits("eternal*"))

            logger.info("deleting....")
            delete("sometopic", uuid)
            withTimeout(100000) {
                repeat(1000) {
                    val resultCount = queryUUIDHits("eternal*")
                    if (resultCount == 0) {
                        logger.info("Delete processed. continuing.")
                        return@withTimeout
                    }
                    delay(100)
                }
            }
            assertEquals(0, queryUUIDHits(uuid))
        }
    }

    private fun queryUUIDHits(query: String): Int {
        val uri = "http://${container.host}:${container.exposedPort}"

        val node = queryUUID(uri, "q=$query")
        logger.info("Query uri: $node")
        val error = node.get("error")
        if (error == null || error is NullNode) {
            return node.get("hits").get("total").get("value").asInt()
        } else {
            return -1
        }
    }

    private fun queryUUID(uri: String, query: String): JsonNode {
        val assembledUri = "$uri/_search?$query"
        logger.info("uuid query: $uri query: $assembledUri")
        val client = HttpClient.newBuilder().build()
        val request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(assembledUri))
            .build()
        val response: HttpResponse<String> = client.send(request, HttpResponse.BodyHandlers.ofString())
        logger.info("return code: ${response.statusCode()}")
        return objectMapper.readTree(response.body())
    }
}
