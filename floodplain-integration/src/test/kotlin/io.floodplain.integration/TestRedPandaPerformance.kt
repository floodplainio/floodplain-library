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

import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.InstantiatedKafkaContainer
import io.floodplain.test.InstantiatedRedPandaContainer
import org.apache.kafka.clients.admin.AdminClient
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runners.MethodSorters
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestRedPandaPerformance {
    // private val kafkaContainer = InstantiatedKafkaContainer()// KafkaContainer("5.5.3").withEmbeddedZookeeper().withExposedPorts(9092)
    // private val containerNetwork = Network.newNetwork()
    // private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag ("5.5.3")).withEmbeddedZookeeper().withExposedPorts(9092)

    //
    @Test
    fun `Environment Warmup`() {
        InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432, mapOf())
    }

    @Test
    fun `Test Red Panda until topic list`() {
        val container = InstantiatedRedPandaContainer()
        val port = container.exposedPort
        val config: MutableMap<String, Any> = HashMap()
        config["bootstrap.servers"] = "localhost:$port" // "localhost:${redPandaContainer.exposedPort}"
        config["client.id"] = UUID.randomUUID().toString()
        val adminClient = AdminClient.create(config)
        val topics = adminClient.listTopics().names().get()
        println("Topics: $topics")
    }

    @Test
    fun `Test Kafka until topic list`() {
        val container = InstantiatedKafkaContainer()
        val port = container.exposedPort
        val config: MutableMap<String, Any> = HashMap()
        config["bootstrap.servers"] = "localhost:$port" // "localhost:${redPandaContainer.exposedPort}"
        config["client.id"] = UUID.randomUUID().toString()
        val adminClient = AdminClient.create(config)
        val topics = adminClient.listTopics().names().get()
        println("Topics: $topics")
    }
}
