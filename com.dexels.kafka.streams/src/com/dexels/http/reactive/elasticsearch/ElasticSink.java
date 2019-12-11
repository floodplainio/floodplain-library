package com.dexels.http.reactive.elasticsearch;

import java.util.Map;
import java.util.Optional;

import org.eclipse.jetty.http.HttpMethod;

import com.dexels.http.reactive.http.HttpInsertTransformer;
import com.dexels.http.reactive.http.JettyClient;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.api.sink.Sink;
import com.dexels.kafka.streams.api.sink.SinkConfiguration;
import com.dexels.replication.api.ReplicationMessage;

import io.reactivex.Completable;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.Function;

public class ElasticSink implements Sink {
	
	@Override
	public FlowableTransformer<ReplicationMessage, Completable> createTransformer(Map<String,String> attributes,
			Optional<SinkConfiguration> streamConfiguration, String instanceName, Optional<String> tenant, String deployment, String generation) {
		if(!streamConfiguration.isPresent()) {
			throw new RuntimeException("Failed configuration: ElasticSink needs config");
		}
		TopologyContext topologyContext = new TopologyContext(tenant, deployment, instanceName, generation);
		Map<String,String> settings = streamConfiguration.get().settings();
		Function<ReplicationMessage,String> indexExtractor = msg->CoreOperators.generationalGroup(attributes.get("index"), topologyContext);
		Function<ReplicationMessage,String> typeExtractor = msg->attributes.get("type");
		String url = settings.get("url");
		return flow->flow.compose(ElasticInsertTransformer.elasticSearchInserter(indexExtractor, typeExtractor, 200, 200))
//				.zipWith(interval, (a,b)->a)
				.compose(HttpInsertTransformer.httpInsert(url,req->req.method(HttpMethod.POST), "application/x-ndjson",1,1,true))
//	        	.compose(HttpInsertTransformer.httpInsert(url,req->req.method(HttpMethod.POST),  "application/x-ndjson",1,1,true))
	        	.map(rep->JettyClient.ignoreReply(rep));
				
//				.compose(JettyClient.responseStream());
	}

}
