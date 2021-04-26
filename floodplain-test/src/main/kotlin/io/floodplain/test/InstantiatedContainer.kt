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
package io.floodplain.test

import com.github.dockerjava.api.command.InspectContainerResponse
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName
import java.nio.charset.StandardCharsets
import java.time.Duration

val useIntegraton: Boolean by lazy {
    System.getenv("NO_INTEGRATION") == null
}

private val logger = mu.KotlinLogging.logger {}

class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(DockerImageName.parse(imageName))

/**
 * Kotlin wrapper, to make testcontainers easier to use
 */
class InstantiatedContainer(
    image: String,
    port: Int,
    env: Map<String, String> = emptyMap(),
    customizer: ((KGenericContainer) -> KGenericContainer)? = null
) {

    var container: KGenericContainer? = KGenericContainer(image)
        .apply { withExposedPorts(port) }
        .apply { withEnv(env) }
        .apply { customizer?.invoke(this) }
    var host: String
    var exposedPort: Int = -1

    init {
        container?.start()
        host = container?.host ?: "localhost"
        exposedPort = container?.firstMappedPort ?: -1
    }

    fun close() {
        container?.close()
    }
}

// KafkaContainer("5.5.3").withEmbeddedZookeeper().withExposedPorts(9092,9093)
class InstantiatedKafkaContainer(customizer: ((KafkaContainer) -> KafkaContainer)? = null) {
    // class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(DockerImageName.parse(imageName))
    val container = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.2"))
        .apply { withExposedPorts(9092, 9093) }
        .apply { withStartupTimeout(Duration.ofMinutes(5)) }
        .apply { withStartupAttempts(50) }
        .apply { customizer?.invoke(this) }
    var host: String
    var exposedPort: Int = -1

    init {
        container.start()
        host = container.host ?: "localhost"
        exposedPort = container.getMappedPort(9093) ?: -1
    }

    fun close() {
        container.close()
    }
}

class InstantiatedRedPandaContainer(customizer: ((RedpandaContainer) -> RedpandaContainer)? = null) {
    private val container = RedpandaContainer()
        .apply { customizer?.invoke(this) }
    var host: String
    var exposedPort: Int = -1

    init {
        container.start()
        host = container.host ?: "localhost"
        exposedPort = container.getMappedPort(9092) ?: -1
    }

    fun close() {
        container.close()
    }
}

// withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:9092");

private const val STARTER_SCRIPT = "/testcontainers_start.sh"

class RedpandaContainer : GenericContainer<RedpandaContainer?>("vectorized/redpanda:v21.4.13") {
    override fun containerIsStarting(containerInfo: InspectContainerResponse?) {
        super.containerIsStarting(containerInfo)
        var command = "#!/bin/bash\n"
        command += "/usr/bin/rpk redpanda start --check=false --node-id 0 "
        command += "--kafka-addr PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092 "
        command += "--advertise-kafka-addr PLAINTEXT://broker:29092,OUTSIDE://$host:" + getMappedPort(
            9092
        )
        logger.info("command: $command")
        logger.info("mapped port: $host:${getMappedPort(9092)}")
        copyFileToContainer(
            Transferable.of(command.toByteArray(StandardCharsets.UTF_8), 511),
            STARTER_SCRIPT
        )
        logger.info("Copied to location: $STARTER_SCRIPT")
    }

    init {
        withExposedPorts(9092)
        withCreateContainerCmdModifier { cmd -> cmd.withEntrypoint("sh") }
        withCommand("-c", "while [ ! -f $STARTER_SCRIPT ]; do sleep 0.1; done; $STARTER_SCRIPT")
        waitingFor(Wait.forLogMessage(".*Started Kafka API server.*", 1))
    }
}
