package com.dexels.navajo.reactive.topology;

import java.util.Stack;

import org.apache.kafka.streams.Topology;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.api.CompiledReactiveScript;
import com.dexels.navajo.reactive.api.ReactivePipe;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;

public class ReactivePipeParser {
	
	
	public static Topology parseReactiveStreamDefinition(CompiledReactiveScript crs, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
		Topology topology = new Topology();
		int pipeNr = 0;
		for (ReactivePipe pipe : crs.pipes) {
			ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, pipeNr,new Stack<String>(), pipe);
		}
		ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology);
		return topology;
	}
	
	public static int processPipe(TopologyContext topologyContext, TopologyConstructor topologyConstructor, Topology topology,
			int pipeNr, Stack<String> pipeStack, ReactivePipe pipe) {
//		String pipeId = "pipe_"+pipeNr++;
//		AtomicInteger pipeNr = new AtomicInteger(initialPipeNr); 
//		System.err.println("source name: "+pipe.source.getClass().getName());
//			pipe.source.execute(context, current, paramMessage)
		int size = pipe.transformers.size();
		for (int i = size; i >= 0; i--) {
			System.err.println(">>> "+i);
			TopologyPipeComponent source = (TopologyPipeComponent)pipe.source;
			if(i==0) {
				System.err.println("processing source");
			} else {
				Object type = pipe.transformers.get(i-1);
				if(type instanceof TopologyPipeComponent) {
					TopologyPipeComponent tpc = (TopologyPipeComponent)type;
					TopologyPipeComponent parent = i-2 < 0 ? source : (TopologyPipeComponent) pipe.transformers.get(i-2);
					System.err.println("processing transformer: "+(i-1));
					if(tpc.materializeParent()) {
						System.err.println("Materializing parent");
						parent.setMaterialize();
					}
					
//					pipeNr = tpc.addToTopology(pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
				} else {
					System.err.println("Weird type found: "+type);
				}
				
			}
			
		}

		TopologyPipeComponent sourceTopologyComponent = (TopologyPipeComponent)pipe.source;
		pipeNr = sourceTopologyComponent.addToTopology(pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
		for (Object e : pipe.transformers) {
			System.err.println("Transformer: "+e);
			if(e instanceof TopologyPipeComponent) {
				TopologyPipeComponent tpc = (TopologyPipeComponent)e;
				pipeNr = tpc.addToTopology(pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
			}
		}

		return pipeNr;
//		pipe.transformers.forEach(e->{
//			if(e instanceof ReactiveTransformer) {
//				ReactiveTransformer rt = (ReactiveTransformer)e;
//				System.err.println("type: "+rt.metadata().name());
//				if(!rt.parameters().named.isEmpty()) {
//					System.err.println("named params:");
//					rt.parameters().named.entrySet().forEach(entry->{
//						System.err.println("param: "+entry.getKey()+" value: "+entry.getValue()+" type: "+entry.getValue().returnType());
//					});
//					System.err.println("|< end of named");
//					
//				}
//				if(!rt.parameters().unnamed.isEmpty()) {
//					rt.parameters().unnamed.forEach(elt->{
//						System.err.println("E: "+elt+" type: "+elt.returnType());
//					});
//				}
//			}
//		});
//		System.err.println("pipe: "+pipe);
	}
	}
