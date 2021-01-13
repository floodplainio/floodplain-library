package io.floodplain.integration

import io.floodplain.test.useIntegraton
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.*
import org.apache.kafka.common.config.ConfigResource.Type.TOPIC
import org.junit.Ignore
import org.junit.Test
import org.testcontainers.containers.KafkaContainer
import java.util.Collections
import java.util.UUID
import java.util.HashMap




private val logger = mu.KotlinLogging.logger {}

class TestTopicCreation {
        // .withEnv(env)

    // Infra for testing Kafka interaction
    @Test @Ignore
    fun testCreateTopic() {
        val kafkaContainer = KafkaContainer("5.5.3").withEmbeddedZookeeper().withExposedPorts(9092,9093)

        kafkaContainer.start()
        logger.info("Bootstrap: ${kafkaContainer.bootstrapServers}")
        val config: MutableMap<String, Any> = HashMap()
        config["bootstrap.servers"] = kafkaContainer.bootstrapServers
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
        val configsResult = adminClient.describeConfigs(cr);
        val cnf = configsResult.all().get()["mytopic"]

//        adminClient.createTopics()
        val configMap: MutableMap<String, String> = HashMap()
        configMap["cleanup.policy"] = "compact"


    //        adminClient.listTopics(ListTopicsOptions())
    }

    // KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)
    // .withNetwork(network)
}