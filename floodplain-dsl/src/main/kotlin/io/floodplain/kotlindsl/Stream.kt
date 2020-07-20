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
import java.net.URL
import java.util.Properties
import java.util.Stack
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.WallclockTimestampExtractor

private val logger = mu.KotlinLogging.logger {}

class Stream(val context: TopologyContext) {

    private val sources: MutableList<Source> = ArrayList()
    private val sinkConfigurations: MutableList<Config> = mutableListOf()
    private val sourceConfigurations: MutableList<Config> = mutableListOf()

    /**
     * Adds a source instance, should only be called from source implementations
     */
    fun addSource(source: Source): Stream {
        sources.add(source)
        return this
    }

    /**
     * Adds a sink config, should only be called from a sink implementation
     */
    fun addSinkConfiguration(c: Config): Config {
        sinkConfigurations.add(c)
        return c
    }

    /**
     * Adds a source config, should only be called from a source implementation
     */
    fun addSourceConfiguration(c: Config) {
        sourceConfigurations.add(c)
    }

    fun sinkConfigurations(): List<Config> {
        return sinkConfigurations.toList()
    }

    fun sourceConfigurations(): List<Config> {
        return sourceConfigurations.toList()
    }

    private fun renderTopology(topologyConstructor: TopologyConstructor): Topology {
        val topology = Topology()
        val reactivePipes = sources.map { e -> e.toReactivePipe() }
        val stack = Stack<String>()
        for (reactivePipe in reactivePipes) {
            ReactivePipeParser.processPipe(context, topologyConstructor, topology, topologyConstructor.generateNewStreamId(), stack, reactivePipe, false)
        }
        ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology)
        return topology
    }
    fun renderAndExecute(applicationId: String? = null, bufferTime: Int, localCmds: suspend LocalContext.() -> Unit) {
            val topologyConstructor = TopologyConstructor()
            val (topology, sources, sinks) = render(topologyConstructor)
            // val offsetPath = Paths.get("offset_" + UUID.randomUUID())
            val sourceConfigs = this@Stream.sourceConfigurations
            val sinkConfigs = this@Stream.sinkConfigurations
            // logger.info("Using offset path: $offsetPath sources: ${ this@Stream.sourceConfigurations.first()}")
            logger.info("Testing topology:\n${topology.describe()}")
            logger.info("Testing sources:\n$sources")
            logger.info("Testing sinks:\n$sinks")
            logger.info("Sourcetopics: \n${topologyConstructor.desiredTopicNames().map { it.qualifiedString(context) }}")

            runLocalTopology(applicationId ?: UUID.randomUUID().toString(),
                bufferTime,
                topology, localCmds, topologyConstructor, context, sourceConfigs, sinkConfigs)
    }

        /**
     * Will create an executable definition of the str
     * eam (@see render), then will start the topology by starting a streams
     * instance pointing at the kafka cluster at kafkaHosts, using the supplied clientId.
     * Finally, it will POST the supplied
     */
    fun renderAndSchedule(connectorURL: URL, kafkaHosts: String, force: Boolean = false): KafkaStreams {
        val topologyConstructor = TopologyConstructor()
        val (topology, sources, sinks) = render(topologyConstructor)
        topologyConstructor.createTopicsAsNeeded(context, kafkaHosts)
        sources.forEach { (name, json) ->
            startConstructor(name, context, connectorURL, json, force)
        }
        sinks.forEach { (name, json) ->
            startConstructor(name, context, connectorURL, json, force)
        }
        val appId = context.topicName("@applicationId")
        val streams = runTopology(topology, appId, kafkaHosts, "storagePath")
        logger.info { "Topology running!" }
        return streams
    }

    /**
     * Creates an executable of a stream definition. Will return three values:
     * - A Streams topology
     * - A list of kafka connect source pairs (name to json definition)
     * - A list of kafka connect sink pairs (name to json definition)
     */
    private fun render(topologyConstructor: TopologyConstructor): Triple<Topology, List<Pair<String, String>>, List<Pair<String, String>>> {
        val topology = renderTopology(topologyConstructor)
        val sources = sourceConfigurations().flatMap { element ->
            element.materializeConnectorConfig(context)
        }.map {
            it.name to constructConnectorJson(context, it.name, it.settings)
        }
        val sinks = sinkConfigurations().flatMap { element ->
            element.materializeConnectorConfig(context)
        }.map {
            it.name to constructConnectorJson(context, it.name, it.settings)
        }
        return Triple(topology, sources, sinks)
    }

    private fun runTopology(topology: Topology, applicationId: String, kafkaHosts: String, storagePath: String): KafkaStreams {
        val props = createProperties(applicationId, kafkaHosts, storagePath)
        val stream = KafkaStreams(topology, props)
        logger.info("CurrentTopology:\n ${topology.describe()}")
        stream.setUncaughtExceptionHandler { thread: Thread, exception: Throwable? ->
            logger.error("Error in streams. thread: ${thread.name} exception: ", exception)
            stream.close()
        }
        stream.setStateListener { newState: KafkaStreams.State?, oldState: KafkaStreams.State? ->
            logger.info("State moving from {} to {}", oldState, newState, stream.state()) }
        stream.start()
        return stream
    }

    private fun createProperties(applicationId: String, brokers: String, storagePath: String): Properties {
        val streamsConfiguration = Properties()
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        logger.info("Creating application with name: {}", applicationId)
        logger.info("Creating application id: {}", applicationId)
        logger.info("Starting instance in storagePath: {}", storagePath)
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = brokers
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = StreamOperators.replicationSerde.javaClass
        streamsConfiguration[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        streamsConfiguration[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = 30000
        streamsConfiguration[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 40000
        streamsConfiguration[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = 5000
        streamsConfiguration[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 7200000
        streamsConfiguration[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 100
        streamsConfiguration[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "lz4"
        streamsConfiguration[StreamsConfig.STATE_DIR_CONFIG] = storagePath
        streamsConfiguration[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = 1
        streamsConfiguration[StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG] = 0
        streamsConfiguration[StreamsConfig.RETRIES_CONFIG] = 50
        streamsConfiguration[StreamsConfig.REPLICATION_FACTOR_CONFIG] = CoreOperators.topicReplicationCount()
        streamsConfiguration[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java

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
            io.floodplain.runtime.run(this@Stream, args, { after(it) }, { kafkaStreams, topologyContext -> after(topologyContext) })
        }
        // io.floodplain.runtime.runWithArguments(this@Stream, arrayOf(*args), { after(it) }, { after() })
    }
}
