package com.dexels.navajo.reactive.topology;

import java.util.Stack;

import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.api.CompiledReactiveScript;
import com.dexels.navajo.reactive.api.ReactivePipe;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;

public class ReactivePipeParser {
	
	
	private final static Logger logger = LoggerFactory.getLogger(ReactivePipeParser.class);

	public static Topology parseReactiveStreamDefinition(Topology topology, CompiledReactiveScript crs, TopologyContext topologyContext, TopologyConstructor topologyConstructor, String namespace) {
		int pipeNr = topologyConstructor.generateNewPipeId();
		for (ReactivePipe pipe : crs.pipes) {
			ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, pipeNr,new Stack<String>(), pipe,false,namespace);
		}
		logger.info("Topology before materialize: {}", topology.describe());
		ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology);
		return topology;
	}
	
	public static void processPipe(TopologyContext topologyContext, TopologyConstructor topologyConstructor, Topology topology,
			int pipeNr, Stack<String> pipeStack, ReactivePipe pipe, boolean materializeTop, String namespace) {
		int size = pipe.transformers.size();
		if(size==0) {
			// no transformers
			if(materializeTop) {
				TopologyPipeComponent source = (TopologyPipeComponent)pipe.source;
				source.setMaterialize();
			}
		} else {
			if(materializeTop) {
				TopologyPipeComponent top = (TopologyPipeComponent) pipe.transformers.get(size-1);
				top.setMaterialize();
			}
		}
		for (int i = size; i >= 0; i--) {
			logger.info(">>> {}", i);
			TopologyPipeComponent source = (TopologyPipeComponent)pipe.source;
			if(i==0) {
				logger.info("processing source");
			} else {
				Object type = pipe.transformers.get(i-1);
				if(type instanceof TopologyPipeComponent) {
					TopologyPipeComponent tpc = (TopologyPipeComponent)type;
					TopologyPipeComponent parent = i-2 < 0 ? source : (TopologyPipeComponent) pipe.transformers.get(i-2);
					logger.info("processing transformer: "+(i-1));
					if(tpc.materializeParent()) {
						logger.info("Materializing parent");
						parent.setMaterialize();
					}
					
//					pipeNr = tpc.addToTopology(pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
				} else {
					logger.warn("Weird type found: {}", type);
				}
				
			}
			
		}

		TopologyPipeComponent sourceTopologyComponent = (TopologyPipeComponent)pipe.source;
		sourceTopologyComponent.addToTopology(namespace, pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
		for (Object e : pipe.transformers) {
			logger.info("Transformer: {} pipestack: {}",e,pipeStack);
			if(e instanceof TopologyPipeComponent) {
				TopologyPipeComponent tpc = (TopologyPipeComponent)e;
				logger.info("Adding pipe component: "+tpc.getClass()+" to stack: "+pipeStack);
				tpc.addToTopology(namespace, pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
			}
		}
	}

}
