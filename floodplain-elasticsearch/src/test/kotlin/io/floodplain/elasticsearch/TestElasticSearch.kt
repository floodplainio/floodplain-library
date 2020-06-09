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

            delay(2000)
            assertEquals(1, queryUUIDHits(uuid))
            delete("sometopic", uuid)
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
