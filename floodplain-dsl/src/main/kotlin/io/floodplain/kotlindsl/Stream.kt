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

import io.floodplain.reactive.topology.ReactivePipeParser
import io.floodplain.streams.api.CoreOperators
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.base.RocksDBConfigurationSetter
import io.floodplain.streams.base.StreamOperators
import io.floodplain.streams.remotejoin.ReplicationTopologyParser
import io.floodplain.streams.remotejoin.TopologyConstructor
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy
import org.apache.kafka.connect.runtime.Connect
import org.apache.kafka.connect.runtime.Herder
import org.apache.kafka.connect.runtime.Worker
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.distributed.DistributedHerder
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.RestServer
import org.apache.kafka.connect.storage.KafkaConfigBackingStore
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore
import org.apache.kafka.connect.storage.KafkaStatusBackingStore
import org.apache.kafka.connect.storage.StatusBackingStore
import org.apache.kafka.connect.util.ConnectUtils
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import java.io.InputStream
import java.net.URI
import java.net.URL
import java.util.Properties
import java.util.Stack
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

class Stream(override val topologyContext: TopologyContext, val topologyConstructor: TopologyConstructor) : FloodplainSourceContainer {

    private val sources: MutableList<Source> = ArrayList()
    private val sinkConfigurations: MutableList<SinkConfig> = mutableListOf()
    private val sourceConfigurations: MutableList<SourceConfig> = mutableListOf()

    private val localSinkConfigurations: MutableList<AbstractSinkConfig> = mutableListOf()

    val tenant: String? = topologyContext.tenant.orElse(null)
    val deployment: String? = topologyContext.deployment.orElse(null)
    val generation: String = topologyContext.generation

    fun topic(name: String): String {
        val buffer = StringBuilder()
        if (tenant != null) {
            buffer.append("$tenant-")
        }
        if (deployment != null) {
            buffer.append("$deployment-")
        }
        buffer.append(name)
        return buffer.toString()
    }

    fun generationalTopic(name: String): String {
        val buffer = StringBuilder()
        if (tenant != null) {
            buffer.append("$tenant-")
        }
        if (deployment != null) {
            buffer.append("$deployment-")
        }
        buffer.append("$generation-")
        buffer.append(name)
        return buffer.toString()
    }

    /**
     * Adds a source instance, should only be called from source implementations
     */
    override fun addSource(source: Source) {
        sources.add(source)
    }

    /**
     * Adds a sink config, should only be called from a sink implementation
     */
    fun addSinkConfiguration(c: SinkConfig): SinkConfig {
        sinkConfigurations.add(c)
        return c
    }

    /**
     * Adds a sink config, should only be called from a sink implementation
     */
    fun addLocalSinkConfiguration(c: AbstractSinkConfig) {
        localSinkConfigurations.add(c)
    }

    /**
     * Adds a source config, should only be called from a source implementation
     */
    fun addSourceConfiguration(c: SourceConfig) {
        sourceConfigurations.add(c)
    }

    private fun sinkConfigurations(): List<SinkConfig> {
        return sinkConfigurations.toList()
    }

    private fun sourceConfigurations(): List<SourceConfig> {
        return sourceConfigurations.toList()
    }

    private fun renderTopology(): Topology {
        val topology = Topology()
        val reactivePipes = sources.map { e -> e.toReactivePipe() }
        val stack = Stack<String>()
        for (reactivePipe in reactivePipes) {
            ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, topologyConstructor.generateNewStreamId(), stack, reactivePipe, false)
        }
        ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology)
        return topology
    }
    fun renderAndExecute(applicationId: String? = null, bufferTime: Int? = null, localCmds: suspend LocalContext.() -> Unit) {
        val (topology, sources, sinks) = render()
        // val offsetPath = Paths.get("offset_" + UUID.randomUUID())
        val sourceConfigs = this@Stream.sourceConfigurations
        val sinkConfigs = this@Stream.sinkConfigurations
        // logger.info("Using offset path: $offsetPath sources: ${ this@Stream.sourceConfigurations.first()}")
        logger.info("Testing topology:\n${topology.describe()}")
        logger.info("Testing sources:\n$sources")
        logger.info("Testing sinks:\n$sinks")
        logger.info("Sourcetopics: \n${topologyConstructor.desiredTopicNames().map { it.qualifiedString() }}")

        runLocalTopology(
            applicationId ?: UUID.randomUUID().toString(),
            bufferTime,
            topology,
            localCmds,
            this,
            topologyConstructor,
            topologyContext,
            sourceConfigs,
            sinkConfigs,
            sinks
        )
    }

    fun renderAndSchedule(connectorURL: URL?, settings: InputStream, force: Boolean = false): KafkaStreams {
        val prop = Properties()
        prop.load(settings)
        return renderAndSchedule(connectorURL, prop[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] as String, force, Utils.propsToStringMap(prop))
    }

    fun renderAndSchedule(connectorURL: URL?, kafkaHosts: String, kafkaUsername: String, kafkaPassword: String, replicationFactor: Int, force: Boolean = false): KafkaStreams {
        val properties = mapOf(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaHosts,
            "security.protocol" to "SASL_SSL",
            "sasl.jaas.config" to "org.apache.kafka.common.security.plain.PlainLoginModule   required username='$kafkaUsername'   password='$kafkaPassword';",
            "sasl.mechanism" to "PLAIN",
            "acks" to "all",
            StreamsConfig.REPLICATION_FACTOR_CONFIG to replicationFactor.toString()
        )
        return renderAndSchedule(connectorURL, kafkaHosts, force, properties)
    }

    /**
     * Will create an executable definition of the str
     * eam (@see render), then will start the topology by starting a streams
     * instance pointing at the kafka cluster at kafkaHosts, using the supplied clientId.
     * Finally, it will POST source and sink connector configurations to the supplied URL
     * If 'force' is true, existing connector configurations will be deleted and recreated, otherwise
     * the new configuration will be ignored and the old configuration will remain.
     */
    fun renderAndSchedule(connectorURL: URL?, kafkaHosts: String, force: Boolean = false, initialSettings: Map<String, String>? = null, monitor: (suspend Stream.(KafkaStreams) -> Unit)? = null): KafkaStreams {
        val (topology, sources, sinks) = render()
        val settings = initialSettings?.toMutableMap() ?: mutableMapOf()
        val replicationFactor = initialSettings?.get(StreamsConfig.REPLICATION_FACTOR_CONFIG) ?: "1"
        settings["bootstrap.servers"] = kafkaHosts
        settings.putIfAbsent("offset.storage.topic", "OFFSET_STORAGE")
        settings.putIfAbsent("config.storage.topic", "CONFIG_STORAGE")
        settings.putIfAbsent("status.storage.topic", "STATUS_STORAGE")
        settings.putIfAbsent("offset.storage.replication.factor", replicationFactor)
        settings.putIfAbsent("offset.storage.partitions", "2")
        settings.putIfAbsent("status.storage.replication.factor", replicationFactor)
        settings.putIfAbsent("status.storage.partitions", "2")
        settings.putIfAbsent("config.storage.replication.factor", replicationFactor)
        settings.putIfAbsent("group.id", "${topologyContext.applicationId()}-connectors")
        topologyConstructor.createTopicsAsNeeded(settings.toMap())
        sources.forEach { (name, json) ->
            connectorURL?.let {
                startConstructor(name, topologyContext, it, json, force)
            }
        }
        sinks.forEach { (name, json) ->
            connectorURL?.let {
                startConstructor(name, topologyContext, it, json, force)
            }
        }
        instantiateLocalSinks(settings)
        val appId = topologyContext.topicName("@applicationId")
        val extra: MutableMap<String, Any> = mutableMapOf()
        extra.putAll(settings)
        val streams = runTopology(topology, appId, kafkaHosts, "storagePath", extra)
        logger.info { "Topology running!" }
        runBlocking {
            monitor?.invoke(this@Stream, streams)
        }
        return streams
    }

    private fun instantiateLocalSinks(settings: Map<String, String>) {
        var herder: Herder? = null
        if (localSinkConfigurations.isNotEmpty()) {
            herder = startLocalConnect(settings)
        }
        var count = 0
        localSinkConfigurations.flatMap {
            it.instantiateSinkElements()
        }.forEach {
            val localSettings = mutableMapOf<String, String>()
            localSettings.putAll(it)
            val name = "conn-${count++}"
            localSettings["name"] = name
            herder?.putConnectorConfig(name, localSettings, true) { err, created ->
                if (err != null) {
                    logger.error("Error creating connector:", err)
                }
                logger.info("Instantiated: ${created?.created()} result: ${created?.result()}")
            }
        }
    }

    private fun extendWorkerProperties(workerProps: MutableMap<String, String>) {
        workerProps["key.converter"] = "org.apache.kafka.connect.json.JsonConverter"
        workerProps["value.converter"] = "org.apache.kafka.connect.json.JsonConverter"
        // workerProps["offset.storage.file.filename"] = "offset"
        // TODO: Using port 8084 now, that might clash. Random? Don't know.
        workerProps[WorkerConfig.LISTENERS_CONFIG] = "http://127.0.0.1:8084"
        val keys = workerProps.keys.toSet()
        keys.filter { it.startsWith("security") || it.startsWith("sasl") || it.startsWith("ssl") || it.startsWith("bootstrap") }
            .forEach { key ->
                workerProps["consumer.$key"] = workerProps[key]!!
                workerProps["producer.$key"] = workerProps[key]!!
            }
    }

    private fun createKafkaOffsetBackingStore(workerConfig: WorkerConfig): KafkaOffsetBackingStore {
        val kafkaOffsetBackingStore = KafkaOffsetBackingStore()
        kafkaOffsetBackingStore.configure(workerConfig)
        kafkaOffsetBackingStore.start()
        return kafkaOffsetBackingStore
    }

    private fun startLocalConnect(initialWorkerProps: Map<String, String>): Herder {
        val workerProps = mutableMapOf<String, String>()
        workerProps.putAll(initialWorkerProps)
        extendWorkerProperties(workerProps)
        val plugins = Plugins(workerProps)
        val config = DistributedConfig(workerProps)
        val time: Time = Time.SYSTEM
        val connectorClientConfigOverridePolicy: ConnectorClientConfigOverridePolicy = plugins.newPlugin(
            config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
            config,
            ConnectorClientConfigOverridePolicy::class.java
        )
        val kafkaClusterId: String = ConnectUtils.lookupKafkaClusterId(config)
        logger.debug("Kafka cluster ID: $kafkaClusterId")

        val offsetBackingStore = createKafkaOffsetBackingStore(config)

        val rest = RestServer(config)
        rest.initializeServer()
        val advertisedUrl: URI = rest.advertisedUrl()

        val workerId: String = advertisedUrl.host.toString() + ":" + advertisedUrl.port
        val worker = Worker(
            workerId,
            time,
            plugins,
            config,
            offsetBackingStore,
            connectorClientConfigOverridePolicy
        )

        val statusBackingStore: StatusBackingStore = KafkaStatusBackingStore(time, worker.internalValueConverter)
        statusBackingStore.configure(config)

        val configBackingStore = KafkaConfigBackingStore(worker.internalValueConverter, config, worker.configTransformer())
        val herder = DistributedHerder(config, time, worker, kafkaClusterId, statusBackingStore, configBackingStore, advertisedUrl.toString(), connectorClientConfigOverridePolicy)

        val connect = Connect(herder, rest)

        connect.start()
        logger.info("Connect started!!")
        return herder
    }

    /**
     * Creates an executable of a stream definition. Will return three values:
     * - A Streams topology
     * - A list of kafka connect source pairs (name to json definition)
     * - A list of kafka connect sink pairs (name to json definition)
     */
    private fun render(): Triple<Topology, List<Pair<String, String>>, List<Pair<String, String>>> {
        val topology = renderTopology()
        val sources = sourceConfigurations().flatMap { element ->
            element.materializeConnectorConfig()
        }.map {
            it.name to constructConnectorJson(topologyContext, it.name, it.settings)
        }
        val sinks = sinkConfigurations().flatMap { element ->
            element.materializeConnectorConfig()
        }.map {
            it.name to constructConnectorJson(topologyContext, it.name, it.settings)
        }
        return Triple(topology, sources, sinks)
    }

    private fun runTopology(topology: Topology, applicationId: String, kafkaHosts: String, storagePath: String, extra: MutableMap<String, Any>): KafkaStreams {
        extra[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaHosts
        extra[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        extra[StreamsConfig.STATE_DIR_CONFIG] = storagePath

        val props = createProperties(extra)
        val stream = KafkaStreams(topology, props)
        logger.info("CurrentTopology:\n ${topology.describe()}")
        stream.setUncaughtExceptionHandler { thread: Thread, exception: Throwable? ->
            logger.error("Error in streams. thread: ${thread.name} exception: ", exception)
            stream.close()
        }
        stream.setStateListener { newState: KafkaStreams.State?, oldState: KafkaStreams.State? ->
            logger.info("State moving from {} to {}", oldState, newState, stream.state())
        }
        stream.start()
        return stream
    }

    private fun createProperties(extra: Map<String, Any>): Properties {
        val streamsConfiguration = Properties()
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.putAll(extra)
        streamsConfiguration.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        streamsConfiguration.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        streamsConfiguration.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StreamOperators.replicationSerde.javaClass)
        streamsConfiguration.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        streamsConfiguration.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000)
        streamsConfiguration.putIfAbsent(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 40000)
        streamsConfiguration.putIfAbsent(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5000)
        streamsConfiguration.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 7200000)
        streamsConfiguration.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100)
        streamsConfiguration.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
        streamsConfiguration.putIfAbsent(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
        streamsConfiguration.putIfAbsent(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0)
        streamsConfiguration.putIfAbsent(StreamsConfig.REPLICATION_FACTOR_CONFIG, CoreOperators.topicReplicationCount())
        streamsConfiguration.putIfAbsent(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor::class.java)

        // 24h for now
        streamsConfiguration["retention.ms"] = 3600 * 24 * 1000

        // 10 weeks for now
        streamsConfiguration["message.timestamp.difference.max.ms"] = 604800000L * 10
        streamsConfiguration["log.message.timestamp.difference.max.ms"] = 604800000L * 11

// 	    StreamsConfig.
        streamsConfiguration[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 10 * 1024 * 1024L

        streamsConfiguration[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 1000
        streamsConfiguration[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = 7900000
        streamsConfiguration[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = 7900000
        streamsConfiguration[StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG] = RocksDBConfigurationSetter::class.java
        return streamsConfiguration
    }

    fun runWithArguments(args: Array<out String?> = emptyArray(), after: suspend ((topologyContext: TopologyContext) -> Unit)) {
        runBlocking {
            io.floodplain.runtime.run(this@Stream, args, { after(it) }, { _, topologyContext -> after(topologyContext) })
        }
    }

    override val rootTopology: Stream
        get() = this
}
