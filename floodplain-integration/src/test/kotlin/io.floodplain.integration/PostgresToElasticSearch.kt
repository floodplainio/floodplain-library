package io.floodplain.integration

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.floodplain.elasticsearch.elasticSearchConfig
import io.floodplain.elasticsearch.elasticSearchSink
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.streams
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test

private val logger = mu.KotlinLogging.logger {}

@kotlinx.coroutines.ExperimentalCoroutinesApi
class TestCombined {

    private val objectMapper = ObjectMapper()

    val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)
    val elasticSearchContainer = InstantiatedContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:7.7.0", 9200, mapOf("discovery.type" to "single-node"))

    @After
    fun shutdown() {
        postgresContainer.close()
        elasticSearchContainer.close()
    }

    /**
     * Test the simplest imaginable pipe: One source and one sink.
     */
    @Test @Ignore
    fun testPostgresSource() {
        println("Logger class: ${logger.underlyingLogger}")
        logger.debug("startdebug")
        streams("any", "myinstance") {
            val postgresConfig = postgresSourceConfig("mypostgres", postgresContainer.host, postgresContainer.exposedPort, "postgres", "mysecretpassword", "dvdrental", "public")
            val elasticConfig = elasticSearchConfig("elastic", "http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}")

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
                    elasticSearchSink("@address", "@address", "@address", elasticConfig)
                },
                postgresConfig.sourceSimple("customer") {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                    }
                    elasticSearchSink("@customer", "@customer", "@customer", elasticConfig)
                },
                postgresConfig.sourceSimple("store") {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                    }
                    elasticSearchSink("@store", "@store", "@store", elasticConfig)
                },
                postgresConfig.sourceSimple("staff") {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                    }
                    elasticSearchSink("@staff", "@staff", "@staff", elasticConfig)
                })
        }.renderAndTest {
            logger.info("Outputs: ${outputs()}")
            val index = topologyContext().topicName("@customer")
            logger.warn("Will query: \"http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}/${index}\"")
            delay(10000)

            // find a customer from Amersfoort. There should be one.
            var hits = 0
            withTimeout(100000) {
                repeat(1000) {
                    try {
                        val node = query("http://${elasticSearchContainer.host}:${elasticSearchContainer.exposedPort}/$index", "q=Amersfoort")
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
            connectJobs().forEach { it.cancel("ciao!") }
            //
            // repeat(1000) {
            //     val outputs = outputs()
            //
            //     outputs.forEach {
            //         logger.info(" Output: $it count: ${outputSize(it)}")
            //     }
            //     delay(10000)
            // }
            // outputs()
            //
            // val index = topologyContext().topicName("@customer")
            // delay(30000)
            // query("http://${postgresContainer.host}:${postgresContainer.exposedPort}/${index}","q=Amersfoort")
            // connectJobs().forEach { it.cancel("ciao!") }
        }
    }

    private fun query(uri: String, query: String): JsonNode {

        val client = HttpClient.newBuilder().build()
        val uri = URI.create("$uri/_search?$query")
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
