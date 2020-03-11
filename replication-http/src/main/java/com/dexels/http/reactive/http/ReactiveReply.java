package com.dexels.http.reactive.http;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.reactive.client.ContentChunk;
import org.eclipse.jetty.reactive.client.ReactiveResponse;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;

public class ReactiveReply {

	public final ReactiveResponse response;
	public final Flowable<ContentChunk> content;
	public final ByteArrayOutputStream debugRequestLog;
	public final AtomicReference<byte[]> requestDump;

	public ReactiveReply(ReactiveResponse response, Publisher<ContentChunk> content, Consumer<byte[]> receivedReporter, ByteArrayOutputStream debugRequestLog, AtomicReference<byte[]> requestDump) {
		this.response = response;
		this.content = Flowable.fromPublisher(content);
		this.debugRequestLog = debugRequestLog;
		this.requestDump = requestDump;
	}

	public Map<String,String> responseHeaders() {
		Map<String,String> rest = new HashMap<>();
		HttpFields headers = response.getHeaders();
		
		headers.getFieldNamesCollection().stream().forEach(ee->{
			rest.put(ee, headers.get(ee) );
		});
		return rest;
	}
	

	
	public int status() {
		return response.getStatus();
	}
}
