package com.dexels.http.reactive.graph;

import java.util.Map;
import java.util.Optional;

import com.dexels.http.reactive.http.JettyClient;
import com.dexels.kafka.streams.api.StreamTopologyException;
import com.dexels.kafka.streams.api.sink.Sink;
import com.dexels.kafka.streams.api.sink.SinkConfiguration;
import com.dexels.replication.api.ReplicationMessage;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.Single;

public class NeoSink implements Sink {

	@Override
	public FlowableTransformer<ReplicationMessage, Completable> createTransformer(Map<String, String> attributes,
			Optional<SinkConfiguration> streamConfiguration, String instanceName, Optional<String> tenant,
			String deployment, String generation) {

		if(!streamConfiguration.isPresent()) {
			throw new StreamTopologyException("Failed configuration: NeoSink needs config");
		}
		Map<String,String> settings = streamConfiguration.get().settings();
		String url = settings.get("url");
		String username = settings.get("username");
		String password = settings.get("password");
		String labels = attributes.get("labels");

		return in->{
			Single<String> cc = in.buffer(2)
					.compose(NeoInsertTransformer.neoNodeTransformer(labels))
					.compose(NeoInsertTransformer.neoInserter(url, username, password))
					.compose(JettyClient.responseStream())
					.reduce(new StringBuilder(),(a,c)->a.append(c))
					.map(e->e.toString())
					.doOnSuccess(e->write(e))
					;
			return Flowable.just(cc.ignoreElement());
		};
	}

	public void write(String resp) {
		System.err.println("RESP: "+resp);
	}
}
