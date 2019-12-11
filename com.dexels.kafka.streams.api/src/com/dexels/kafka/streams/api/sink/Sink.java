package com.dexels.kafka.streams.api.sink;

import java.util.Map;
import java.util.Optional;

import com.dexels.replication.api.ReplicationMessage;

import io.reactivex.Completable;
import io.reactivex.FlowableTransformer;

public interface Sink {

	public FlowableTransformer<ReplicationMessage, Completable> createTransformer(Map<String, String> attributes,
			Optional<SinkConfiguration> streamConfiguration, String instanceName, Optional<String> tenant, String deployment,
			String generation);

}