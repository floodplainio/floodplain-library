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

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration

val useIntegraton: Boolean by lazy {
    System.getenv("NO_INTEGRATION") == null
}

class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(DockerImageName.parse(imageName))

/**
 * Kotlin wrapper, to make testcontainers easier to use
 */
class InstantiatedContainer(image: String, port: Int, env: Map<String, String> = emptyMap(), customizer: ((KGenericContainer) -> KGenericContainer)? = null) {

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
