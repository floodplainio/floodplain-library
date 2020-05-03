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
package io.floodplain.kotlindsl

import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.fromImmutable
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.replication.factory.ReplicationFactory
import io.floodplain.streams.api.CoreOperators
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.serializer.ReplicationMessageSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.KeyValueStore
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Properties
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

interface TestCommand {
    fun execute()
}

class InputCommand : TestCommand {
    override fun execute() {
    }
}

interface TestContext {
    fun input(topic: String, key: String, msg: IMessage)
    fun delete(topic: String, key: String)
    fun output(topic: String): Pair<String, IMessage>
    fun outputSize(topic: String): Long
    fun deleted(topic: String): String
    fun isEmpty(topic: String): Boolean
    fun advanceWallClockTime(duration: Duration)
    fun stateStore(name: String): KeyValueStore<String, ReplicationMessage>
    fun getStateStoreNames(): Set<String>
}
fun testTopology(topology: Topology, testCmds: TestContext.() -> Unit, context: TopologyContext) {
    val storageFolder = "teststorage/store-" + UUID.randomUUID().toString()
    val props = Properties()
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "doesntmatter:9092")
    props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor::class.java.name)
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ReplicationMessageSerde::class.java.name)
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "doesntmatterid")
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, storageFolder)

    val driver = TopologyTestDriver(topology, props)
    val contextInstance = TestDriverContext(driver, context)
    logger.info("FOLDER: {}", storageFolder)
    try {
        testCmds.invoke(contextInstance)
    } finally {
        driver.allStateStores.forEach { store -> store.value.close() }
        driver.close()
        var path = Path.of(storageFolder).toAbsolutePath()
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach { Files.deleteIfExists(it) }
        }
        // Files.deleteIfExists(path)
    }

//        driver.createInputTopic("",Serdes.String().serializer(),ReplicationMessageSerde().serializer()).
}
class TestDriverContext(private val driver: TopologyTestDriver, private val topologyContext: TopologyContext) : TestContext {

    private val inputTopics = mutableMapOf<String, TestInputTopic<String, ReplicationMessage>>()
    private val outputTopics = mutableMapOf<String, TestOutputTopic<String, ReplicationMessage>>()

    private val replicationMessageParser = ReplicationFactory.getInstance()

    override fun input(topic: String, key: String, msg: IMessage) {
        val qualifiedTopicName = CoreOperators.topicName(topic, topologyContext)
        val inputTopic = inputTopics.computeIfAbsent(qualifiedTopicName) { driver.createInputTopic(qualifiedTopicName, Serdes.String().serializer(), ReplicationMessageSerde().serializer()) }
        inputTopic.pipeInput(key, ReplicationFactory.standardMessage(msg.toImmutable()))
    }

    override fun delete(topic: String, key: String) {
        val qualifiedTopicName = CoreOperators.topicName(topic, topologyContext)
        val inputTopic = inputTopics.computeIfAbsent(qualifiedTopicName) { driver.createInputTopic(qualifiedTopicName, Serdes.String().serializer(), ReplicationMessageSerde().serializer()) }
        inputTopic.pipeInput(key, null)
    }

    override fun output(topic: String): Pair<String, IMessage> {
        val qualifiedTopicName = CoreOperators.topicName(topic, topologyContext)
        val outputTopic = outputTopics.computeIfAbsent(qualifiedTopicName) { driver.createOutputTopic(qualifiedTopicName, Serdes.String().deserializer(), ReplicationMessageSerde().deserializer()) }
        val keyVal: KeyValue<String, ReplicationMessage?> = outputTopic.readKeyValue()
//        outputTopic.
        val op = keyVal.value?.operation() ?: ReplicationMessage.Operation.DELETE
        if (op.equals(ReplicationMessage.Operation.DELETE)) {
            logger.info("delete detected! isnull? ${keyVal.value}")
            logger.info("retrying...")
            return output(topic)
//            throw RuntimeException("Expected message for key: ${keyVal.key}, but got a delete.")
        } else {
            return Pair(keyVal.key, fromImmutable(keyVal.value!!.message()))
        }
    }

    override fun outputSize(topic: String): Long {
        val qualifiedTopicName = CoreOperators.topicName(topic, topologyContext)
        val outputTopic = outputTopics.computeIfAbsent(qualifiedTopicName) { driver.createOutputTopic(qualifiedTopicName, Serdes.String().deserializer(), ReplicationMessageSerde().deserializer()) }
        return outputTopic.queueSize
    }

    override fun deleted(topic: String): String {
        val qualifiedTopicName = CoreOperators.topicName(topic, topologyContext)
        val outputTopic = outputTopics.computeIfAbsent(qualifiedTopicName) { driver.createOutputTopic(qualifiedTopicName, Serdes.String().deserializer(), ReplicationMessageSerde().deserializer()) }
        logger.info("Looking for a tombstone message for topic $qualifiedTopicName")
        val keyVal = outputTopic.readKeyValue()
        logger.info { "Found key ${keyVal.key} operation: ${keyVal.value?.operation()}" }
        if (keyVal.value != null) {
            if (keyVal.value.operation() == ReplicationMessage.Operation.DELETE) {
                return deleted(topic)
            }
            logger.error { "Unexpected content: ${replicationMessageParser.describe(keyVal.value)} remaining queue: ${outputTopic.queueSize}" }
            throw RuntimeException("Expected delete message for key: ${keyVal.key}, but got a value: ${keyVal.value}")
        }
        return keyVal.key
    }

    override fun isEmpty(topic: String): Boolean {
        val qualifiedTopicName = CoreOperators.topicName(topic, topologyContext)
        val outputTopic = outputTopics.computeIfAbsent(qualifiedTopicName) { driver.createOutputTopic(qualifiedTopicName, Serdes.String().deserializer(), ReplicationMessageSerde().deserializer()) }
        return outputTopic.isEmpty
    }

    override fun stateStore(name: String): KeyValueStore<String, ReplicationMessage> {
        return driver.getKeyValueStore(name)
    }

    override fun getStateStoreNames(): Set<String> {
        return driver.allStateStores.keys
    }

    override fun advanceWallClockTime(duration: Duration) {
        driver.advanceWallClockTime(duration)
    }
}
