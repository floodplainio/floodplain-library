package io.floodplain.integration

import org.apache.kafka.clients.admin.AdminClient
import org.junit.Ignore
import org.junit.Test
import java.util.HashMap
import java.util.UUID

class TestRedPanda {
    // private val redPandaContainer = InstantiatedContainer("vectorized/redpanda:latest", 9092)

    // Not working TODO implement
    @Test @Ignore
    fun testSimplePanda() {
        // val port = redPandaContainer.exposedPort
        val config: MutableMap<String, Any> = HashMap()
        config["bootstrap.servers"] = "localhost:9092" // "localhost:${redPandaContainer.exposedPort}"
        config["client.id"] = UUID.randomUUID().toString()
        val adminClient = AdminClient.create(config)
        val topics = adminClient.listTopics().names().get()
        println("Topics: $topics")
    }
}
