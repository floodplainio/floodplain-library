package io.floodplain.integration

import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.InstantiatedRedPandaContainer
import io.floodplain.test.REDPANDA_IMAGE
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.Network

private val logger = mu.KotlinLogging.logger {}

class TestRedPandaDebezium {
    // Test just Red Panda + Debezium (and no floodplain / streams)
    // Just to troubleshoot some inital issues. If RedPanda works well over time this test can be removed
    //vectorized/redpanda:v21.4.12
    private val containerNetwork = Network.newNetwork()
    private val kafkaContainer = InstantiatedRedPandaContainer(REDPANDA_IMAGE) { container->
        container.withNetwork(containerNetwork)!!.withNetworkAliases("broker")!!
    }
    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432, mapOf()) {
        it.withNetwork(
            containerNetwork
        ).withNetworkAliases("postgres")
    }
    private var debeziumContainer: InstantiatedContainer? = null

    @BeforeAll
    fun setup() {
        logger.info("Created network: ${containerNetwork.id}")
        val bootstrap = "${kafkaContainer.host}:${kafkaContainer.exposedPort}"
        logger.info("kafka.getBootstrapServers(): bootstrap: $bootstrap")

        debeziumContainer = InstantiatedContainer(
            "debezium/connect:1.5",
            8083,
            mapOf(
                "BOOTSTRAP_SERVERS" to "broker:29092",
                "CONFIG_STORAGE_TOPIC" to "CONNECTOR_STORAGE",
                "OFFSET_STORAGE_TOPIC" to "OFFSET_STORAGE"
            )
        ) {
            it.withNetwork(containerNetwork)
                .withNetworkAliases("debezium")
        }
        debeziumContainer?.container?.start()
        logger.info("Setup done")
        Thread.sleep(20000)
    }

    // This test should be removed, it is totally occluded by other tests, it is just an easier target to troubleshoot
    // container<->container + host<->container network connectivity issues
    @Test
    fun test() {
        // Thread.sleep(2000000)

    }
}
