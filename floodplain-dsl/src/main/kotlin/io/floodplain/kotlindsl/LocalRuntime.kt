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

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.floodplain.bufferTimeout
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.fromImmutable
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.replication.factory.ReplicationFactory
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import io.floodplain.streams.serializer.ReplicationMessageSerde
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.connect.json.JsonDeserializer
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
import java.time.Instant
import java.util.Collections
import java.util.Optional
import java.util.Properties

private val logger = mu.KotlinLogging.logger {}

interface InputReceiver: FloodplainOperator {
    fun input(topic: Topic, key: String, msg: IMessage)
    fun inputQualified(topic: String, key: String, msg: IMessage)

    fun input(topic: String, key: ByteArray, msg: ByteArray)
    fun inputQualified(topic: String, key: ByteArray, msg: ByteArray)
    fun delete(topic: String, key: String)
    fun deleteQualified(topic: String, key: String)
    fun inputs(): Set<String>
}

interface LocalContext : InputReceiver {
    fun output(topic: String): Pair<String, IMessage>
    fun outputQualified(topic: String): Pair<String, IMessage>
    fun skip(topic: String, number: Int)
    /**
     * Doesn't work, outputs get created lazily, so only sinks that have been accessed before are counted
     */
    fun outputs(): Set<String>
    fun outputSize(topic: String): Long
    fun deleted(topic: String): String
    fun deletedQualified(topic: String): String
    fun isEmpty(topic: String): Boolean
    fun isEmptyQualified(topic: String): Boolean
    fun advanceWallClockTime(duration: Duration)
    fun stateStore(name: String): KeyValueStore<String, ReplicationMessage>
    fun getStateStoreNames(): Set<String>
    fun topologyContext(): TopologyContext
    fun topologyConstructor(): TopologyConstructor
    fun sourceConfigurations(): List<SourceConfig>
    fun sinkConfigurations(): List<SinkConfig>
    fun connectJobs(): List<Job>
    fun flushSinks()
    suspend fun connectSource()

    // fun outputFlow(): Map<Topic, Flow<Pair<String, IMessage?>>>
    // override fun outputFlow(): Flow<List<Triple<Topic, String, IMessage?>>> {
    // fun sinkConsumer(sinks: Map<Topic, List<FloodplainSink>>): (Pair<Topic, List<Pair<String, IMessage?>>>) -> Unit
    fun sinksByTopic(): Map<Topic, List<FloodplainSink>>
    fun connectSourceAndSink(): List<Job>
}

fun runLocalTopology(
    applicationId: String,
    bufferTime: Int?,
    topology: Topology,
    localCmds: suspend LocalContext.() -> Unit,
    rootTopology: Stream,
    topologyConstructor: TopologyConstructor,
    context: TopologyContext,
    sourceConfigs: List<SourceConfig>,
    sinkConfigs: List<SinkConfig>,
    sinks: List<Pair<String, String>>
) {
    val storageFolder = "teststorage/store"
    val props = Properties()
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "doesntmatter:9092")
    props.setProperty(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        WallclockTimestampExtractor::class.java.name
    )
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ReplicationMessageSerde::class.java.name)
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, storageFolder)
    props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST)
    val driver = TopologyTestDriver(topology, props)

    val contextInstance = LocalDriverContext(driver, rootTopology, context, topologyConstructor, Utils.propsToStringMap(props), sourceConfigs, sinkConfigs, sinks, bufferTime)
    val jobs = contextInstance.connectSourceAndSink()
    contextInstance.connectJobs.addAll(jobs)
    try {
        runBlocking {
            localCmds(contextInstance)
            contextInstance.closeSinks()
        }
    } finally {

        driver.allStateStores.forEach { store -> store.value.close() }
        driver.close()
        // TODO Add switch to be able to resume (so don't delete state files)?
        val path = Path.of(storageFolder).toAbsolutePath()
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.deleteIfExists(it) }
        }
    }
}

class LocalDriverContext(
    private val driver: TopologyTestDriver,
    override val rootTopology: Stream,
    override val topologyContext: TopologyContext,
    private val topologyConstructor: TopologyConstructor,
    private val config: Map<String,String>,
    private val sourceConfigs: List<SourceConfig>,
    private val sinkConfigs: List<SinkConfig>,
    private val sinks: List<Pair<String, String>>,
    private val bufferTime: Int?
) : LocalContext {

    val connectJobs = mutableListOf<Job>()
    private val inputTopics = mutableMapOf<String, TestInputTopic<String, ReplicationMessage>>()
    // private val rawInputTopics = mutableMapOf<String, TestInputTopic<String, ByteArray>>()

    private val outputTopics = mutableMapOf<String, TestOutputTopic<String, ReplicationMessage>>()

    private val replicationMessageParser = ReplicationFactory.getInstance()
    override fun sourceConfigurations(): List<SourceConfig> {
        return sourceConfigs
    }

    override fun sinkConfigurations(): List<SinkConfig> {
        return sinkConfigs
    }

    override fun connectJobs(): List<Job> {
        return connectJobs
    }

    override fun inputs(): Set<String> {
        return inputTopics.keys // + rawInputTopics.keys
    }

    override fun outputs(): Set<String> {
        return outputTopics.keys
    }

    override suspend fun connectSource() {
        sourceConfigs.forEach { config ->
            config.connectSource(this@LocalDriverContext)
        }
    }

    private fun <T> Flow<T>.handleErrors(): Flow<T> =
        catch { e -> logger.error("Error in flow $e") }

    @OptIn(kotlinx.coroutines.ObsoleteCoroutinesApi::class)
    override fun connectSourceAndSink(): List<Job> {
        val outputJob = GlobalScope.launch(newSingleThreadContext("TopologySource"), CoroutineStart.UNDISPATCHED) {
            val outputFlows = outputFlows(this)
                .map { (topic, flow) -> topic to flow.bufferTimeout(2000, bufferTime?.toLong() ?: 1000) }
                .toMap()
            val sinks = sinksByTopic()
            outputFlows.forEach { (topic, flow) ->
                logger.info("Connecting topic $topic to sink: ${sinks[topic]}")
                this.launch {
                    flow.collect { elements ->
                        sinks[topic]?.forEach {
                            // logger.info("Buffered. Sending size: ${elements.size} topic: $topic")
                            it.send(topic, elements)
                        }
                    }
                }
            }
        }
        outputJob.start()
        logger.info("output job connected")
        val inputJob = GlobalScope.launch {
            connectSource()
        }
        logger.info("input job connected")
        return listOf(inputJob, outputJob)
    }

    private fun topics(): Set<Topic> {
        return sinkConfigurations().flatMap { it.materializeConnectorConfig() }.flatMap { it.topics }.toSet()
        // .materializeConnectorConfig(topologyContext) .flatMap { it.sinkElements().keys }
    }

    override fun flushSinks() {
        this.sinkConfigurations().flatMap { config ->
            config.sinkElements().values
        }.flatMap { it }
            .forEach { it.flush() }
    }

    fun closeSinks() {
        this.sinkConfigurations().flatMap { config ->
            config.sinkElements().values
        }.flatten()
            .forEach { it.close() }
    }

    override fun sinksByTopic(): Map<Topic, List<FloodplainSink>> {
        val result = mutableMapOf<Topic, MutableList<FloodplainSink>>()
        this.sinkConfigurations().map {
            instantiateSinkConfig(it)

            // it.instantiateSinkElements()
            // it.sinkElements().entries
        }.forEach { entry ->
            entry.entries.forEach { (key,value)->
                val list = result.computeIfAbsent(key) { mutableListOf() }
                list.addAll(value)
            }
        }
        return result
    }

    // TODO Why not imessage?
    private fun outputFlows(context: CoroutineScope): Map<Topic, Flow<Pair<String, Map<String, Any>?>>> {
        val topics = topics()
        val deserializer = JsonDeserializer() // TODO protobuf issue, topic is not in json connect
        val mapper = ObjectMapper()
        mapper.findAndRegisterModules()
        val fallback = FallbackReplicationMessageParser()
        val sourceFlow = outputFlowSingle()
            .map { (topic, key, value) ->
                // if(value!=null) {
                //     val v = String(value)
                //     logger.info("PPP: $v")
                // }

                // val parsed = if (value == null) null else deserializer.deserialize(topic.qualifiedString(),value)

                // fallback.parseBytes(Optional.ofNullable(topic.qualifiedString()),value).valueMap(false,
                //     Collections.emptySet())
                val parsed = if (value == null) null else fallback.parseBytes(Optional.ofNullable(topic.qualifiedString()), value).valueMap(
                    false,
                    Collections.emptySet()
                )

                val result = if (value == null) null else mapper.convertValue(parsed, object : TypeReference<Map<String, Any>>() {})
                Triple(topic, key, result)
            }
            .shareIn(context, SharingStarted.Lazily)
            // .asFlow()
            .handleErrors()
        return topics.map { topic ->
            val flow = sourceFlow.filter { (incomingTopic, _, _) ->
                incomingTopic == topic
            }.map { (_, key, value) -> key to value }

            topic to flow
        }.toMap()
    }

    private fun outputFlowSingle(): Flow<Triple<Topic, String, ByteArray?>> {
        return callbackFlow {
            driver.setOutputListener { record ->
                // Ignore changelog topics
                if (!record.topic().endsWith("changelog")) {
                    val key = Serdes.String().deserializer().deserialize(record.topic(), record.key())
                    val topic = Topic.fromQualified(record.topic(), topologyContext)
                    if (this.isActive) {
                        sendBlocking(Triple(topic, key, record.value()))
                    }
                }
            }
            logger.info("Outputflow connected!")
            awaitClose {
                logger.info("closing output flow!")
            }
        }
    }

    override fun input(topic: Topic, key: String, msg: IMessage) {
        inputQualified(topic.qualifiedString(), key, msg)
    }

    override fun inputQualified(topic: String, key: String, msg: IMessage) {
        val qualifiedTopicName = topic // topologyContext.topicName(topic)
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

    override fun input(topic: String, key: ByteArray, msg: ByteArray) {
        val qualifiedTopicName = topologyContext.topicName(topic)
        inputQualified(qualifiedTopicName, key, msg)
    }

    override fun inputQualified(topic: String, key: ByteArray, msg: ByteArray) {
        val qualifiedTopicName = topic // topologyContext.topicName(topic)
        if (!inputs().contains(qualifiedTopicName)) {
            logger.debug("Missing topic: $topic available topics: ${inputs()}")
        }
        try {
            driver.pipeRawRecord(qualifiedTopicName, Instant.now().toEpochMilli(), key, msg)
        } catch (e: Throwable) {
            logger.error("Error sending input data", e)
        }
    }

    override fun delete(topic: String, key: String) {
        val qualifiedTopicName = topologyContext.topicName(topic)
        deleteQualified(qualifiedTopicName, key)
    }

    override fun deleteQualified(topic: String, key: String) {
        val inputTopic = inputTopics.computeIfAbsent(topic) {
            driver.createInputTopic(
                topic,
                Serdes.String().serializer(),
                ReplicationMessageSerde().serializer()
            )
        }
        inputTopic.pipeInput(key, null)
    }

    override fun output(topic: String): Pair<String, IMessage> {
        return outputQualified(topologyContext.topicName(topic))
    }
    override fun outputQualified(qualifiedTopicName: String): Pair<String, IMessage> {
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
            output(qualifiedTopicName)
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
        return deletedQualified(topologyContext.topicName(topic))
    }
    override fun deletedQualified(topic: String): String {
        val outputTopic = outputTopics.computeIfAbsent(topic) {
            driver.createOutputTopic(
                topic,
                Serdes.String().deserializer(),
                ReplicationMessageSerde().deserializer()
            )
        }
        logger.info("Looking for a tombstone message for topic $topic")
        val keyVal = outputTopic.readKeyValue()
        logger.info { "Found key ${keyVal.key} operation: ${keyVal.value?.operation()}" }
        if (keyVal.value != null) {
            if (keyVal.value.operation() == ReplicationMessage.Operation.DELETE) {
                return deletedQualified(topic)
            }
            logger.error { "Unexpected content: ${replicationMessageParser.describe(keyVal.value)} remaining queue: ${outputTopic.queueSize}" }
            throw RuntimeException("Expected delete message for key: ${keyVal.key}, but got a value: ${keyVal.value}")
        }
        return keyVal.key
    }

    override fun isEmpty(topic: String): Boolean {
        return isEmptyQualified(topologyContext.topicName(topic))
    }
    override fun isEmptyQualified(topic: String): Boolean {
        val outputTopic = outputTopics.computeIfAbsent(topic) {
            driver.createOutputTopic(
                topic,
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
