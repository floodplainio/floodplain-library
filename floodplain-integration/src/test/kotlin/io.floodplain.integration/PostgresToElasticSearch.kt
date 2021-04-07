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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.floodplain.elasticsearch.elasticSearchConfig
import io.floodplain.elasticsearch.elasticSearchSink
import io.floodplain.kotlindsl.from
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.to
import io.floodplain.kotlindsl.stream
import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.useIntegraton
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

private val logger = mu.KotlinLogging.logger {}

@kotlinx.coroutines.ExperimentalCoroutinesApi
class PostgresToElasticSearch {

    private val objectMapper = ObjectMapper()

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)
    private val elasticSearchContainer = InstantiatedContainer(
        "docker.elastic.co/elasticsearch/elasticsearch-oss:7.7.0",
        9200,
        mapOf("discovery.type" to "single-node")
    )

    @After
    fun shutdown() {
        postgresContainer.close()
        elasticSearchContainer.close()
    }

    @Test
    fun testPostgresToElastic() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests; doesn't seem to work in circleci")
            return
        }
        logger.debug("startdebug")
        stream {
            val postgresConfig = postgresSourceConfig("mypostgres", postgresContainer.host, postgresContainer.exposedPort, "postgres", "mysecretpassword", "dvdrental", "public")
            val elasticConfig = elasticSearchConfig(
                "elastic",
                "http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}"
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
                // elasticSearchSink("@address", "@address", "@address", elasticConfig)
            }
            postgresSource("customer", postgresConfig) {
                joinRemote({ m -> "${m["address_id"]}" }, false) {
                    from("$generation-address") {}
                }
                set { _, msg, state ->
                    msg.set("address", state)
                }
                elasticSearchSink("$generation-customer", "$generation-customer", elasticConfig)
            }
            postgresSource("store", postgresConfig) {
                joinRemote({ m -> "${m["address_id"]}" }, false) {
                    from("$generation-address") {}
                }
                set { _, msg, state ->
                    msg.set("address", state)
                }
                elasticSearchSink("$generation-store", "$generation-store", elasticConfig)
            }
            postgresSource("staff", postgresConfig) {
                joinRemote({ m -> "${m["address_id"]}" }, false) {
                    from("$generation-address") {}
                }
                set { _, msg, state ->
                    msg.set("address", state)
                }
                elasticSearchSink("@staff", "@staff", elasticConfig)
            }
        }.renderAndExecute {
            logger.info("Outputs: ${outputs()}")
            val index = topologyContext().topicName("@customer")
            logger.warn(
                "Will query: \"http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}/${index}\""
            )
            // delay(10000)

            // find a customer from Amersfoort. There should be one.
            var hits = 0
            withTimeout(200000) {
                repeat(1000) {
                    try {
                        val node = query(
                            "http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}/$index",
                            "q=Amersfoort"
                        )
                        logger.info("Resulting node: {}", node)
                        val found = node.get("hits")?.get("total")?.get("value")?.asInt()
                        if (found != null && found > 0) {
                            hits = found
                            return@withTimeout
                        }
                    } catch (e: Throwable) {
                        logger.error("Error checking elasticsearch: ", e)
                    }
                    delay(1000)
                }
            }
            Assert.assertEquals(1, hits)
            // We've found our hit. Close down connections.
            // delay(1000000)
            connectJobs().forEach { it.cancel("ciao!") }
        }
    }

    @Test @Ignore
    fun testPostgresToElasticSimple() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests, doesn't seem to work in circleci")
            return
        }
        logger.debug("startdebug")
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
            val elasticConfig = elasticSearchConfig(
                "elastic",
                "http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}"
            )
            postgresSource("customer", postgresConfig) {
                joinRemote({ m -> "${m["address_id"]}" }, false) {
                    postgresSource("address", postgresConfig) {}
                }
                set { _, msg, state ->
                    msg.set("address", state)
                }
                elasticSearchSink("@customer", "@customer", elasticConfig)
            }
            postgresSource("staff", postgresConfig) {
                elasticSearchSink("@staff", "@staff", elasticConfig)
            }
        }.renderAndExecute {
            logger.info("Outputs: ${outputs()}")
            val index = topologyContext().topicName("@customer")
            logger.warn("Will query: \"http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}/${index}\"")
            delay(10000)

            // find a customer from Chungo. There should be one.
            var hits = 0
            withTimeout(200000) {
                repeat(1000) {
                    try {
                        val node = query(
                            "http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}/$index",
                            "q=*Chungho*"
                        )
                        logger.info("Resulting node: {}", node)
                        val found = node.get("hits")?.get("total")?.get("value")?.asInt()
                        if (found != null && found > 0) {
                            hits = found
                            return@withTimeout
                        }
                    } catch (e: Throwable) {
                        logger.error("Error checking elasticsearch: ", e)
                    }
                    delay(1000)
                }
            }
            Assert.assertEquals(1, hits)
            logger.info("COMPLETED, waiting")
            delay(1000000)
            connectJobs().forEach { it.cancel("ciao!") }
        }
    }

    private fun query(queryUri: String, query: String): JsonNode {

        val client = HttpClient.newBuilder().build()
        val uri = URI.create("$queryUri/_search?$query")
        logger.warn("QUERYING: $uri")
        val request = HttpRequest.newBuilder()
            .GET()
            .uri(uri)
            .timeout(Duration.ofSeconds(10))
            .build()
        val response: HttpResponse<String> = client.send(request, HttpResponse.BodyHandlers.ofString())
        return objectMapper.readTree(response.body())
    }
}
