package io.floodplain.kotlindsl

import com.dexels.kafka.streams.api.CoreOperators
import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import com.dexels.navajo.reactive.topology.ReactivePipeParser
import org.apache.kafka.streams.Topology
import java.net.URL
import java.util.*
import kotlin.collections.ArrayList

class Pipe(val context: TopologyContext, private val topologyConstructor: TopologyConstructor) {

    private val sources: MutableList<Source> = ArrayList()
    private val sinkConfigurations: MutableList<Config> = mutableListOf()
    private val sourceConfigurations: MutableList<Config> = mutableListOf()
    fun addSource(source: Source) {
        sources.add(source)
    }


    fun addSinkConfiguration(c: Config) {
        sinkConfigurations.add(c)
    }

    fun addSourceConfiguration(c: Config) {
        sourceConfigurations.add(c)
    }


    fun sinkConfigurations(): List<Config> {
        return sinkConfigurations.toList()
    }

    fun sourceConfigurations(): List<Config> {
        return sourceConfigurations.toList()
    }

    fun renderTopology(): Topology {
        val topology = Topology()
        val reactivePipes = sources.map { e -> e.toReactivePipe() }
        val stack = Stack<String>()
        for (reactivePipe in reactivePipes) {
            ReactivePipeParser.processPipe(context, topologyConstructor, topology, topologyConstructor.generateNewPipeId(), stack, reactivePipe, true)
        }
        ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology)
        return topology;
    }

    fun renderAndStart(connectorURL:URL, kafkaHosts: String, clientId: String) {
        val (topology,sources,sinks) = render()
        topologyConstructor.createTopicsAsNeeded(kafkaHosts,clientId)
        sources.forEach { (name, json) ->
            startConstructor(name,context, connectorURL,json,true  )
        }
        // TODO does this work?
//        Thread.sleep(5000)
        sinks.forEach { (name, json) ->
            startConstructor(name,context, connectorURL,json,true  )
        }
        val appId = CoreOperators.generationalGroup("appId",context)
        runTopology(topology,appId,kafkaHosts,"storagePath")
    }

    fun render(): Triple<Topology,List<Pair<String,String>>,List<Pair<String,String>>> {
        val topology = renderTopology()
        val sources = sourceConfigurations().map {
            element->
            val (name,config) = element.materializeConnectorConfig(context)
            name to constructConnectorJson(context,name,config)
        }
        val sinks = sinkConfigurations().map {
            element->
            val (name,config) = element.materializeConnectorConfig(context)
            name to constructConnectorJson(context,name,config)
        }
        return Triple(topology,sources,sinks)

    }
}