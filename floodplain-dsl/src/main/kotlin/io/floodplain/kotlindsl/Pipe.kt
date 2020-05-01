package io.floodplain.kotlindsl

import io.floodplain.reactive.topology.ReactivePipeParser
import io.floodplain.streams.api.CoreOperators
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.base.RocksDBConfigurationSetter
import io.floodplain.streams.base.StreamOperators
import io.floodplain.streams.remotejoin.ReplicationTopologyParser
import io.floodplain.streams.remotejoin.TopologyConstructor
import java.io.IOException
import java.net.URL
import java.util.Properties
import java.util.Stack
import kotlin.collections.ArrayList
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.WallclockTimestampExtractor

private val logger = mu.KotlinLogging.logger {}

class Pipe(val context: TopologyContext) {

    private val sources: MutableList<Source> = ArrayList()
    private val sinkConfigurations: MutableList<Config> = mutableListOf()
    private val sourceConfigurations: MutableList<Config> = mutableListOf()

    /**
     * Adds a source instance, should only be called from source implementations
     */
    fun addSource(source: Source): Pipe {
        sources.add(source)
        return this
    }

    /**
     * Adds a sink config, should only be called from a sink implementation
     */
    fun addSinkConfiguration(c: Config) {
        sinkConfigurations.add(c)
    }

    /**
     * Adds a source config, should only be called from a source implementation
     */
    fun addSourceConfiguration(c: Config) {
        sourceConfigurations.add(c)
    }

    private fun sinkConfigurations(): List<Config> {
        return sinkConfigurations.toList()
    }

    private fun sourceConfigurations(): List<Config> {
        return sourceConfigurations.toList()
    }

    private fun renderTopology(topologyConstructor: TopologyConstructor): Topology {
        val topology = Topology()
        val reactivePipes = sources.map { e -> e.toReactivePipe() }
        val stack = Stack<String>()
        for (reactivePipe in reactivePipes) {
            ReactivePipeParser.processPipe(context, topologyConstructor, topology, topologyConstructor.generateNewPipeId(), stack, reactivePipe, false)
        }
        ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology)
        return topology
    }

    fun renderAndTest(testCmds: TestContext.() -> Unit): Pipe {
        val top = renderTopology(TopologyConstructor())
        logger.info("Testing topology:\n${top.describe()}")
        testTopology(top, testCmds, context)
        return this
    }

    /**
     * Will create an executable definition of the stream (@see render), then will start the topology by starting a streams
     * instance pointing at the kafka cluster at kafkaHosts, using the supplied clientId.
     * Finally, it will POST the supplied
     */
    fun renderAndStart(connectorURL: URL, kafkaHosts: String, clientId: String) {
        val topologyConstructor = TopologyConstructor()
        val (topology, sources, sinks) = render(topologyConstructor)
        topologyConstructor.createTopicsAsNeeded(kafkaHosts, clientId)
        sources.forEach { (name, json) ->
            startConstructor(name, context, connectorURL, json, true)
        }
        sinks.forEach { (name, json) ->
            startConstructor(name, context, connectorURL, json, true)
        }
        val appId = CoreOperators.generationalGroup("appId", context)
        runTopology(topology, appId, kafkaHosts, "storagePath")
        logger.info { "Topology running!" }
    }

    /**
     * Creates an executable of a stream definition. Will return three values:
     * - A Streams topology
     * - A list of kafka connect source pairs (name to json definition)
     * - A list of kafka connect sink pairs (name to json definition)
     */
    fun render(topologyConstructor: TopologyConstructor): Triple<Topology, List<Pair<String, String>>, List<Pair<String, String>>> {
        val topology = renderTopology(topologyConstructor)
        val sources = sourceConfigurations().map { element ->
            val (name, config) = element.materializeConnectorConfig(context)
            name to constructConnectorJson(context, name, config)
        }
        val sinks = sinkConfigurations().map { element ->
            val (name, config) = element.materializeConnectorConfig(context)
            name to constructConnectorJson(context, name, config)
        }
        return Triple(topology, sources, sinks)
    }

    @Throws(InterruptedException::class, IOException::class)
    fun runTopology(topology: Topology, applicationId: String, kafkaHosts: String, storagePath: String): KafkaStreams? {
        val props = createProperties(applicationId, kafkaHosts, storagePath)
        val stream = KafkaStreams(topology, props)
        println("CurrentTopology:\n ${topology.describe()}")
        stream.setUncaughtExceptionHandler { thread: Thread, exception: Throwable? ->
            logger.error("Error in streams. thread: ${thread.name} exception: ", exception)
            stream.close()
        }
        stream.setStateListener { newState: KafkaStreams.State?, oldState: KafkaStreams.State? ->
            logger.info("State moving from {} to {}", oldState, newState, stream.state()) }
        stream.start()
        return stream
    }

    private fun createProperties(applicationId: String, brokers: String, storagePath: String): Properties? {
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
}
