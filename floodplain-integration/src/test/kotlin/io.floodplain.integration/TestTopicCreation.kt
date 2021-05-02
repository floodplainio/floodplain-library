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

import io.floodplain.test.InstantiatedRedPandaContainer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.TOPIC
import org.junit.Assert
import org.junit.Test
import java.util.Collections
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

class TestTopicCreation {
    // .withEnv(env)

    // Infra for testing Kafka interaction
    private val panda = InstantiatedRedPandaContainer()
    // val kafkaContainer = KafkaContainer("5.5.3").withEmbeddedZookeeper().withExposedPorts(9092,9093)

    // Not functional yet
    @Test
    fun testCreateTopic() {

        // kafkaContainer.start()
        // logger.info("Bootsx§trap: ${kafkaContainer.bootstrapServers}")
        val config: MutableMap<String, Any> = HashMap()
        val exposedPort = panda.exposedPort
        val host = "localhost:$exposedPort"
        logger.info("Exposed host: $host")
        config["bootstrap.servers"] = host // "localhost:51347"
        config["client.id"] = UUID.randomUUID().toString()
        val adminClient = AdminClient.create(config)
        adminClient.listTopics().names().get().forEach {
            println("topic found: $it")
        }
        // val options = CreateTopicsOptions()
        adminClient.createTopics(listOf(NewTopic("mytopic", 1, 1))).all().get()
        logger.info("${adminClient.describeTopics(listOf("mytopic")).all().get()["mytopic"]}")
        val cr = Collections.singleton(ConfigResource(TOPIC, "mytopic"))
        val configsResult = adminClient.describeConfigs(cr)
        val cnf = configsResult.all().get()
        logger.info("Config: $cnf")
        val configMap: MutableMap<String, String> = HashMap()
        configMap["cleanup.policy"] = "compact"

        Assert.assertEquals(1, adminClient.listTopics(ListTopicsOptions()).names().get().size)
    }
}
