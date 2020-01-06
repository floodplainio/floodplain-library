package com.dexels.http.reactive.elasticsearch;

import org.reactivestreams.Publisher;

import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.Function;

public class ElasticInsertTransformer implements FlowableTransformer<ReplicationMessage,Flowable<byte[]>> {

	private final ObjectMapper objectMapper;
	private Function<ReplicationMessage, String> indexExtractor;
	private Function<ReplicationMessage, String> typeExtractor;
	private final int bufferSize;
	
//	application/x-ndjson
	
	private ElasticInsertTransformer(Function<ReplicationMessage, String> indexExtractor, Function<ReplicationMessage, String> typeExtractor, int bufferSize)  {
		this.objectMapper = new ObjectMapper();
		this.indexExtractor = indexExtractor;
		this.typeExtractor = typeExtractor;
		this.bufferSize = bufferSize;
	}

	public static FlowableTransformer<ReplicationMessage,Flowable<byte[]>> elasticSearchInserter(Function<ReplicationMessage, String> indexExtractor, Function<ReplicationMessage, String> typeExtractor, int bufferSize) {
		return new ElasticInsertTransformer(indexExtractor,typeExtractor, bufferSize);
	}

	private byte[] ndJsonLines(ReplicationMessage msg) throws Exception {
		String key = msg.combinedKey();
		ObjectNode root = objectMapper.createObjectNode();
		ObjectNode node = objectMapper.createObjectNode(); //
		//
		node.put("_index", this.indexExtractor.apply(msg).toLowerCase());
		node.put("_type", this.typeExtractor.apply(msg).toLowerCase());
		node.put("_id", key);
		root.set("index",node);
		String idLine = objectMapper.writeValueAsString(root);
		StringBuilder out = new StringBuilder(idLine);
		out.append('\n');
		out.append(ImmutableFactory.ndJson(msg.message()));
		out.append('\n');
		return out.toString().getBytes();
	}

	@Override
	public Publisher<Flowable<byte[]>> apply(Flowable<ReplicationMessage> flow) {
		return flow
				.map(this::ndJsonLines)
				.buffer(bufferSize)
				.filter(b->b.size()>0)
				.map(e->Flowable.fromIterable(e));
				
	}
	
	
	

}
