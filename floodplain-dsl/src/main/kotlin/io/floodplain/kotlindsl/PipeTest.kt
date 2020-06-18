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

import io.floodplain.bufferTimeout
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.fromImmutable
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.replication.api.ReplicationMessageParser
import io.floodplain.replication.factory.ReplicationFactory
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import io.floodplain.streams.serializer.ReplicationMessageSerde
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Optional
import java.util.Properties
import java.util.UUID
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.broadcastIn
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.KeyValueStore

private val logger = mu.KotlinLogging.logger {}

private val parser: ReplicationMessageParser = JSONReplicationMessageParserImpl()

interface InputReceiver {
    fun input(topic: String, key: String, msg: IMessage)

    // fun input(topic: String, key: String, msg: ByteArray)
    fun delete(topic: String, key: String)
    fun inputs(): Set<String>
}

interface TestContext : InputReceiver {
    fun output(topic: String): Pair<String, IMessage>
    fun skip(topic: String, number: Int)

    /**
     * Doesn't work, outputs get created lazily, so only sinks that have been accessed before are counted
     */
    fun outputs(): Set<String>
    fun outputSize(topic: String): Long
    fun deleted(topic: String): String
    fun isEmpty(topic: String): Boolean
    fun advanceWallClockTime(duration: Duration)
    fun stateStore(name: String): KeyValueStore<String, ReplicationMessage>
    fun getStateStoreNames(): Set<String>
    fun topologyContext(): TopologyContext
    fun topologyConstructor(): TopologyConstructor
    fun sourceConfigurations(): List<Config>
    fun sinkConfigurations(): List<Config>
    fun connectJobs(): List<Job>
    fun flushSinks()
    suspend fun connectSource()

    // fun outputFlow(): Map<Topic, Flow<Pair<String, IMessage?>>>
    // override fun outputFlow(): Flow<List<Triple<Topic, String, IMessage?>>> {
    // fun sinkConsumer(sinks: Map<Topic, List<FloodplainSink>>): (Pair<Topic, List<Pair<String, IMessage?>>>) -> Unit
    fun sinksByTopic(): MutableMap<Topic, MutableList<FloodplainSink>>
    fun connectSourceAndSink(): List<Job>
}

fun testTopology(
    topology: Topology,
    testCmds: suspend
    TestContext.() -> Unit,
    topologyConstructor: TopologyConstructor,
    context: TopologyContext,
    sourceConfigs: List<Config>,
    sinkConfigs: List<Config>
) {
    val storageFolder = "teststorage/store-" + UUID.randomUUID().toString()
    val props = Properties()
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "doesntmatter:9092")
    props.setProperty(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        WallclockTimestampExtractor::class.java.name
    )
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ReplicationMessageSerde::class.java.name)
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "doesntmatterid")
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, storageFolder)

    val driver = TopologyTestDriver(topology, props)
    val contextInstance = TestDriverContext(driver, context, topologyConstructor, sourceConfigs, sinkConfigs)
    val jobs = contextInstance.connectSourceAndSink()
    contextInstance.connectJobs.addAll(jobs)
    try {
        runBlocking {
            testCmds(contextInstance)
            contextInstance.closeSinks()
        }
    } finally {
        driver.allStateStores.forEach { store -> store.value.close() }
        driver.close()
        val path = Path.of(storageFolder).toAbsolutePath()
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.deleteIfExists(it) }
        }
    }
}

class TestDriverContext(
    private val driver: TopologyTestDriver,
    private val topologyContext: TopologyContext,
    private val topologyConstructor: TopologyConstructor,
    private val sourceConfigs: List<Config>,
    private val sinkConfigs: List<Config>
) : TestContext {

    val connectJobs = mutableListOf<Job>()
    private val inputTopics = mutableMapOf<String, TestInputTopic<String, ReplicationMessage>>()
    private val outputTopics = mutableMapOf<String, TestOutputTopic<String, ReplicationMessage>>()

    private val replicationMessageParser = ReplicationFactory.getInstance()
    override fun sourceConfigurations(): List<Config> {
        return sourceConfigs
    }

    override fun sinkConfigurations(): List<Config> {
        return sinkConfigs
    }

    override fun connectJobs(): List<Job> {
        return connectJobs
    }

    override fun inputs(): Set<String> {
        return inputTopics.keys
    }

    override fun outputs(): Set<String> {
        return outputTopics.keys
    }

    override suspend fun connectSource() {
        sourceConfigs.forEach { config ->
            config.connectSource(this@TestDriverContext)
        }
    }

    override fun connectSourceAndSink(): List<Job> {
        val outputJob = GlobalScope.launch {
            val outputFlows = outputFlows(this)
                .onEach { (k, _) -> logger.info("Topic mapped: $k") }
                .map { (topic, flow) -> topic to flow.bufferTimeout(200, 2000) }
                .toMap()
            val sinks = sinksByTopic()
            outputFlows.forEach { (topic, flow) ->
                logger.info("Connecting topic $topic to sink: ${sinks[topic]}")
                this.async {
                    flow.collect { elements ->
                        sinks[topic]?.forEach {
                            logger.info("Buffered. Sending size: ${elements.size} topic: $topic")
                            it.send(topic, elements)
                        }
                    }
                }
            }
        }
        val inputJob = GlobalScope.launch {
            connectSource()
        }
        return listOf(inputJob, outputJob)
    }

    private fun topics(): Set<Topic> {
        return sinkConfigurations().flatMap { it.sinkElements().keys }.toSet()
    }

    // private fun topicSinks(topic: Topic) {
    //     return sinkConfigurations().flatMap { it.sinkElements().keys }.toSet()
    // }

    override fun flushSinks() {
        this.sinkConfigurations().map { config ->
            config.sinkElements()
        }.forEach { configSinks ->
            configSinks.map { sink -> sink.value.flush() }
        }
    }

    fun closeSinks() {
        this.sinkConfigurations().map { config ->
            config.sinkElements()
        }.forEach { configSinks ->
            configSinks.map { sink -> sink.value.close() }
        }
    }

    // fun sinkConsumer(): (List<Triple<Topic, String, IMessage?>>) -> Unit {
    //     val map = mutableMapOf<Topic, MutableList<FloodplainSink>>()
    //     this.sinkConfigurations().map {
    //         it.sinkElements()
    //     }.forEach {
    //             sinkElements -> sinkElements.forEach {
    //             (topic, message) ->
    //         map.computeIfAbsent(topic) {
    //             mutableListOf()
    //         }.add(message)
    //     }
    //     }
    //     return sinkConsumer(map)
    // }

    override fun sinksByTopic(): MutableMap<Topic, MutableList<FloodplainSink>> {
        val map = mutableMapOf<Topic, MutableList<FloodplainSink>>()
        this.sinkConfigurations().map {
            it.sinkElements()
        }.forEach { sinkElements ->
            sinkElements.forEach { (topic, sink) ->
                map.computeIfAbsent(topic) {
                    mutableListOf()
                }.add(sink)
            }
        }
        return map
        // return sinkConsumer(map)
    }

    private fun outputFlows(context: CoroutineScope): Map<Topic, Flow<Pair<String, IMessage?>>> {
        val topics = topics()
        // GlobalScope.launch {
        val sourceFlow = outputFlowSingle().broadcastIn(context).asFlow()
        return topics.map { topic ->
            val flow = sourceFlow.filter { (incomingTopic, _, _) ->
                incomingTopic == topic
            }.map { (_, key, value) -> key to value }

            topic to flow
        }.toMap()
    }

    private fun outputFlowSingle(): Flow<Triple<Topic, String, IMessage?>> {
        return callbackFlow<Triple<Topic, String, IMessage?>> {
            driver.setOutputListener {
                val key = Serdes.String().deserializer().deserialize(it.topic(), it.key())
                val message = parser.parseBytes(Optional.of(it.topic()), it.value())
                val imessage = message?.message()?.let { it1 -> fromImmutable(it1) }
                val topic = Topic.fromQualified(it.topic())
                if (this.isActive) {
                    // topics[topic]!!.sendBlocking(key to imessage)
                    sendBlocking(Triple(topic, key, imessage))
                }
            }
            awaitClose {
                println("closing output flow!")
            }
        }
    }

    override fun input(topic: String, key: String, msg: IMessage) {
        val qualifiedTopicName = topologyContext.topicName(topic)
        if (!inputs().contains(qualifiedTopicName)) {
            logger.debug("Missing topic: $topic available topics: ${inputs()}")
        }
        val inputTopic = inputTopics.computeIfAbsent(qualifiedTopicName) {
            driver.createInputTopic(
                qualifiedTopicName,
                Serdes.String().serializer(),
                ReplicationMessageSerde().serializer()
            )
        }
        val replicationMsg = ReplicationFactory.standardMessage(msg.toImmutable())
        inputTopic.pipeInput(key, replicationMsg)
    }

    override fun delete(topic: String, key: String) {
        val qualifiedTopicName = topologyContext.topicName(topic)
        val inputTopic = inputTopics.computeIfAbsent(qualifiedTopicName) {
            driver.createInputTopic(
                qualifiedTopicName,
                Serdes.String().serializer(),
                ReplicationMessageSerde().serializer()
            )
        }
        inputTopic.pipeInput(key, null)
    }

    override fun output(topic: String): Pair<String, IMessage> {
        val qualifiedTopicName = topologyContext.topicName(topic)
        val outputTopic = outputTopics.computeIfAbsent(qualifiedTopicName) {
            driver.createOutputTopic(
                qualifiedTopicName,
                Serdes.String().deserializer(),
                ReplicationMessageSerde().deserializer()
            )
        }
        val keyVal: KeyValue<String, ReplicationMessage?> = outputTopic.readKeyValue()
        val op = keyVal.value?.operation() ?: ReplicationMessage.Operation.DELETE
        return if (op == ReplicationMessage.Operation.DELETE) {
            logger.info("delete detected! isnull? ${keyVal.value}")
            logger.info("retrying...")
            output(topic)
        } else {
            Pair(keyVal.key, fromImmutable(keyVal.value!!.message()))
        }
    }

    override fun skip(topic: String, number: Int) {
        repeat(number) {
            output(topic)
        }
    }

    override fun outputSize(topic: String): Long {
        val qualifiedTopicName = topologyContext.topicName(topic)
        val outputTopic = outputTopics.computeIfAbsent(qualifiedTopicName) {
            driver.createOutputTopic(
                qualifiedTopicName,
                Serdes.String().deserializer(),
                ReplicationMessageSerde().deserializer()
            )
        }
        return outputTopic.queueSize
    }

    override fun deleted(topic: String): String {
        val qualifiedTopicName = topologyContext.topicName(topic)
        val outputTopic = outputTopics.computeIfAbsent(qualifiedTopicName) {
            driver.createOutputTopic(
                qualifiedTopicName,
                Serdes.String().deserializer(),
                ReplicationMessageSerde().deserializer()
            )
        }
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
        val qualifiedTopicName = topologyContext.topicName(topic)
        val outputTopic = outputTopics.computeIfAbsent(qualifiedTopicName) {
            driver.createOutputTopic(
                qualifiedTopicName,
                Serdes.String().deserializer(),
                ReplicationMessageSerde().deserializer()
            )
        }
        return outputTopic.isEmpty
    }

    override fun stateStore(name: String): KeyValueStore<String, ReplicationMessage> {
        val kv: KeyValueStore<String, ReplicationMessage>? = driver.getKeyValueStore(name)
        if (kv == null) {
            val stores = driver.allStateStores.map { (k, _) -> k as String }.toList()
            logger.error("Can't find state store. Available stores: $stores")
            throw IllegalStateException("Missing state store: $name")
        }
        return kv
    }

    override fun getStateStoreNames(): Set<String> {
        return driver.allStateStores.keys
    }

    override fun topologyContext(): TopologyContext {
        return topologyContext
    }

    override fun topologyConstructor(): TopologyConstructor {
        return topologyConstructor
    }

    override fun advanceWallClockTime(duration: Duration) {
        driver.advanceWallClockTime(duration)
    }
}
