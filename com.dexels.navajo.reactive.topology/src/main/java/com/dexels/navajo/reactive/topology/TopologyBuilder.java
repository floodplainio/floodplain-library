package com.dexels.navajo.reactive.topology;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.streams.Topology;

import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.parser.compiled.ParseException;
import com.dexels.navajo.reactive.ReactiveStandalone;
import com.dexels.navajo.reactive.api.CompiledReactiveScript;
import com.dexels.navajo.reactive.api.ReactivePipe;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveTransformer;

@ApplicationScoped
public class TopologyBuilder {
	
//	getClass().getResourceAsStream("simpletopic.rr")

	public CompiledReactiveScript parseScript(InputStream is) throws ParseException, IOException {
		return ReactiveStandalone.compileReactiveScript(is);
	}
	public void buildTopology(CompiledReactiveScript crs,TopologyContext topologyContext,TopologyConstructor topologyConstructor) {
		Topology topology = new Topology();
		for (ReactivePipe pipe : crs.pipes) {
			System.err.println("source name: "+pipe.source.getClass().getName());
			parseTopology(pipe,topology,topologyContext,topologyConstructor);
			System.err.println("pipe: "+pipe);
		}

	}
	private void parseTopology(ReactivePipe pipe,Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor) {
		ReactiveSource source = pipe.source;
//		TopologyBuilder
//		public StreamScriptContext(String tenant, String service, String deployment) {
//			this(UUID.randomUUID().toString(),System.currentTimeMillis(),tenant,service,Optional.empty(),NavajoFactory.getInstance().createNavajo(),Collections.emptyMap(),Optional.empty(),Optional.empty(),Collections.emptyList(),Optional.empty(),Optional.empty(),Optional.empty());
//			this.deployment = Optional.ofNullable(deployment);
//		}
		// TODO address default?
		StreamScriptContext c = new StreamScriptContext(topologyContext.tenant.orElse("DEFAULT"), topologyContext.instance, topologyContext.deployment);
		ReactiveResolvedParameters resolved = source.parameters().resolve(c, Optional.empty(), ImmutableFactory.empty(),source.metadata());
		ReplicationTopologyParser.addSourceStore(topology, topologyContext, topologyConstructor, Optional.empty(), resolved.paramString("name"), Optional.empty(),false);
		pipe.transformers.forEach(e->{
			System.err.println("Transformer: "+e);
			if(e instanceof ReactiveTransformer) {
				ReactiveTransformer rt = (ReactiveTransformer)e;
				//
				System.err.println("type: "+rt.metadata().name());
				if(!rt.parameters().named.isEmpty()) {
					System.err.println("named params:");
					rt.parameters().named.entrySet().forEach(entry->{
						System.err.println("param: "+entry.getKey()+" value: "+entry.getValue()+" type: "+entry.getValue().returnType());
					});
					System.err.println("|< end of named");
					
				}
				if(!rt.parameters().unnamed.isEmpty()) {
					rt.parameters().unnamed.forEach(elt->{
						System.err.println("E: "+elt+" type: "+elt.returnType());
					});
				}
			}
		});
	}
	

	public void testBuild() {
		
	}
}
