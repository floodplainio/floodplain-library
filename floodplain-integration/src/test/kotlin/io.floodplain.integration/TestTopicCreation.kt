package io.floodplain.integration

import io.floodplain.test.RedPandaContainer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.TOPIC
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test
import java.util.Collections
import java.util.UUID
import java.util.HashMap




private val logger = mu.KotlinLogging.logger {}

class TestTopicCreation {
        // .withEnv(env)

    // Infra for testing Kafka interaction
    private val panda = RedPandaContainer("vectorized/redpanda:latest", 9092)
    // val kafkaContainer = KafkaContainer("5.5.3").withEmbeddedZookeeper().withExposedPorts(9092,9093)

    // Not functional yet
    @Test @Ignore
    fun testCreateTopic() {


        // kafkaContainer.start()
        // logger.info("Bootsx§trap: ${kafkaContainer.bootstrapServers}")
        val config: MutableMap<String, Any> = HashMap()
        // val exposedPort = panda.exposedPort
        val host = "localhost:${panda.exposedPort}"
        logger.info("Exposed host: $host")
        config["bootstrap.servers"] = host //"localhost:51347"
        config["client.id"] = UUID.randomUUID().toString()
        val adminClient = AdminClient.create(config)
        adminClient.listTopics().names().get().forEach {
            println("topic found: $it")
        }
        val options = CreateTopicsOptions()
        var createResult = adminClient.createTopics(listOf(NewTopic("mytopic",1,1))).all().get()
        val topicDescription = adminClient.describeTopics(listOf("mytopic")).all().get()["mytopic"]
    //    topicDescription?
        val cr =  Collections.singleton(ConfigResource(TOPIC, "mytopic"))
        val configsResult = adminClient.describeConfigs(cr)
        val cnf = configsResult.all().get()["mytopic"]

//        adminClient.createTopics()
        val configMap: MutableMap<String, String> = HashMap()
        configMap["cleanup.policy"] = "compact"


        Assert.assertEquals(1,adminClient.listTopics(ListTopicsOptions()).names().get().size)

    }

    // KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)
    // .withNetwork(network)
}