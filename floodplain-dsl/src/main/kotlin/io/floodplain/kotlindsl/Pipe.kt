package io.floodplain.kotlindsl

import io.floodplain.reactive.topology.ReactivePipeParser
import io.floodplain.streams.api.CoreOperators
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.base.StreamInstance
import io.floodplain.streams.remotejoin.ReplicationTopologyParser
import io.floodplain.streams.remotejoin.TopologyConstructor
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.Topology
import java.io.IOException
import java.net.URL
import java.util.*
import kotlin.collections.ArrayList

private val logger = mu.KotlinLogging.logger {}

class Pipe(val context: TopologyContext, private val topologyConstructor: TopologyConstructor) {

    private val sources: MutableList<Source> = ArrayList()
    private val sinkConfigurations: MutableList<Config> = mutableListOf()
    private val sourceConfigurations: MutableList<Config> = mutableListOf()

    /**
     * Adds a source instance, should only be called from source implementations
     */
    fun addSource(source: Source) {
        sources.add(source)
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

    private fun renderTopology(): Topology {
        val topology = Topology()
        val reactivePipes = sources.map { e -> e.toReactivePipe() }
        val stack = Stack<String>()
        for (reactivePipe in reactivePipes) {
            ReactivePipeParser.processPipe(context, topologyConstructor, topology, topologyConstructor.generateNewPipeId(), stack, reactivePipe, true)
        }
        ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology)
        return topology;
    }

    /**
     * Will create an executable definition of the stream (@see render), then will start the topology by starting a streams
     * instance pointing at the kafka cluster at kafkaHosts, using the supplied clientId.
     * Finally, it will POST the supplied
     */
    fun renderAndStart(connectorURL: URL, kafkaHosts: String, clientId: String) {
        val (topology, sources, sinks) = render()
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
    fun render(): Triple<Topology, List<Pair<String, String>>, List<Pair<String, String>>> {
        val topology = renderTopology()
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
        val props = StreamInstance.createProperties(applicationId, kafkaHosts, storagePath)
        val stream = KafkaStreams(topology, props)
        println("CurrentTopology:\n ${topology.describe()}")
        stream.setUncaughtExceptionHandler { thread: Thread, exception: Throwable? ->
            logger.error("Error in streams. thread: ${thread.name} exception: ", exception)
            stream.close()
        }
        stream.setStateListener { oldState: KafkaStreams.State?, newState: KafkaStreams.State? -> logger.info("State moving from {} to {}", oldState, newState) }
        stream.start()
        return stream
    }
}