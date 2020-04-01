package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Stack;

public class LogTransformer implements TopologyPipeComponent {

	private final String logName;
//	private final Optional<Integer> every;
	private final boolean dumpStack;
	private boolean materialize = false;

	
	private final static Logger logger = LoggerFactory.getLogger(LogTransformer.class);

	public static final String SINK_PREFIX = "SINK_";
	public static final String SINKLOG_PREFIX = "SINK_LOG_";
	public LogTransformer(String logName, Optional<Integer> every, boolean dumpStack) {
		this.logName = logName;
//		this.every = every;
		this.dumpStack = dumpStack;
	}

	@Override
	public void addToTopology(Stack<String> transformerNames, int pipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor) {
//		boolean dumpStack = resolved.optionalBoolean("dumpStack").orElse(false);
//		if(every.isPresent()) {
//			throw new UnsupportedOperationException("'every' param not yet implemented in LogTransformer");
//		}
//		String logName = resolved.paramString("logName");
		logger.info("Stack top for transformer: "+transformerNames.peek());
//		String name = createName(topologyContext, transformerNames.size(), pipeId);
		String name = topologyContext.qualifiedName("log",transformerNames.size(), pipeId);
		if (this.materialize()) {
//			topology.addProcessor(filterName+"_prematerialize",filterProcessor, transformerNames.peek());
			topology.addProcessor(name+"_prematerialize", ()->new LogProcessor(logName,dumpStack), transformerNames.peek());
			ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, name, name+"_prematerialize");
		} else {
			topology.addProcessor(name, ()->new LogProcessor(logName,dumpStack), transformerNames.peek());
		}
		transformerNames.push(name);
	}

	@Override
	public boolean materializeParent() {
		return false;
	}
	@Override
	public void setMaterialize() {
		this.materialize  = true;
	}

	@Override
	public boolean materialize() {
		return this.materialize;
	}

}
