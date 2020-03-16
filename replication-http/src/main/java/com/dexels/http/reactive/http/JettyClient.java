package com.dexels.http.reactive.http;

import io.reactivex.*;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.reactive.client.ContentChunk;
import org.eclipse.jetty.reactive.client.ReactiveRequest;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class JettyClient {

	private final HttpClient httpClient = new HttpClient(new SslContextFactory());
	private AtomicLong sent = new AtomicLong();
	private final AtomicInteger concurrent = new AtomicInteger(); 
	
	private final static Logger logger = LoggerFactory.getLogger(JettyClient.class);

	public JettyClient() {
		try {
			httpClient.start();
		} catch (Exception e) {
			logger.error("Error: ", e);
		}
	}

	public Single<ReactiveReply> callWithoutBody(String uri, Function<Request,Request> buildRequest) {
		return call(uri,buildRequest, Optional.empty(), Optional.empty(),false);
	}

	public Flowable<byte[]> callWithoutBodyToStream(String uri, Function<Request,Request> buildRequest) {
		return call(uri,buildRequest, Optional.empty(), Optional.empty(),false)
				.toFlowable()
				.compose(JettyClient.responseStream());
	}
	
	public Single<ReactiveReply> callWithBody(String uri, Function<Request,Request> buildRequest,Flowable<byte[]> requestBody,String requestContentType) {
		return call(uri,buildRequest,Optional.of(requestBody),Optional.of(requestContentType),false);
	}
	public Flowable<byte[]> callWithBodyToStream(String uri, Function<Request,Request> buildRequest,Flowable<byte[]> requestBody,String requestContentType) {
		return call(uri,buildRequest,Optional.of(requestBody),Optional.of(requestContentType),false)
				.toFlowable()
				.compose(JettyClient.responseStream())
				;
	}
	
	public FlowableTransformer<Flowable<byte[]>, ReactiveReply> call(String uri,Function<Request,Request> buildRequest,Optional<String> requestContentType, int maxConcurrent, int prefetch, boolean debugMode) {
		return item->item.concatMap(e->{
			return call(uri,buildRequest,Optional.of(e),requestContentType, debugMode).toFlowable();
		});
	}
	public  Single<ReactiveReply> call(String uri,Function<Request,Request> buildRequest,Optional<Flowable<byte[]>> requestBody,Optional<String> requestContentType, boolean debugMode) {
		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		
		Request req = httpClient.newRequest(uri);
		Request reqProcessed = buildRequest.apply(req);
		ReactiveRequest.Builder requestBuilder = ReactiveRequest.newBuilder(reqProcessed);
		if(requestContentType.isPresent()) {
			reqProcessed = reqProcessed.header("Content-Type", requestContentType.get());
		}
		final ByteArrayOutputStream requestDump = new ByteArrayOutputStream();
		AtomicReference<byte[]> debugRequestBody = new AtomicReference<byte[]>();
		if(requestBody.isPresent()) {
			Publisher<ContentChunk> bb = requestBody.get()
					.doOnNext(e->{
						if(debugMode) {
							boas.write(e);
						}
					})
					.doOnNext(b->this.sent.addAndGet(b.length))
					.doOnNext(e->{
						if(debugMode) {
							requestDump.write(e);
						}
					})
					.doOnComplete(()->{
						final byte[] byteArray = requestDump.toByteArray();
						logger.info("Total request size: "+byteArray.length);
						debugRequestBody.set(byteArray);
						
					})
					.map(e->new ContentChunk(ByteBuffer.wrap(e)));
			requestBuilder = requestBuilder.content(ReactiveRequest.Content.fromPublisher(bb, requestContentType.get()));
		}
		ReactiveRequest request = requestBuilder.build();
		return Single.fromPublisher(request.response((response, content) -> Flowable.just(new ReactiveReply(response,content,b->this.sent.addAndGet(b.length),requestDump,debugRequestBody))
				.doOnError(e->{
					logger.error("|HTTP call to url: "+uri+" failed: ",e);
					if(debugMode) {
						byte[] data = requestDump.toByteArray();
						logger.error("|Failed request. Size: {}",data.length);
						logger.error("|Request: "+new String(data));
					}
				})))
				.doOnError(e->{
					logger.error("HTTP call to url: "+uri+" failed: ",e);
					if(debugMode) {
						byte[] data = requestDump.toByteArray();
						logger.error("Failed request. Size: {}",data.length);
						logger.error("Request: "+new String(data));
					}
				})
				.doOnSuccess(
						(reply)->logger.info("HTTP Client to {}: sent: {}",uri,sent.get())
				)
				.doOnSubscribe(e->{
					int con = concurrent.incrementAndGet();
					System.err.println("Concurrent inc: "+con+"-> "+uri);
				})
				.doFinally(()->{
					int con = concurrent.decrementAndGet();
					System.err.println("Concurrent dec: "+con+"-> "+uri);
					
				});
	}

	public static FlowableTransformer<ReactiveReply, byte[]> responseStream() {
		return single->single.flatMap(e->e.content).flatMap(c->JettyClient.streamResponse(c));
	}

	public static Completable ignoreReply(ReactiveReply reply) {
		System.err.println("Response:"+reply.response.getResponse().getStatus());
		return reply.content.flatMap(e->streamResponse(e)).ignoreElements();
//		reply.content.blockingForEach(e->{
//			System.err.println("Item: "+e);
//		});
//		return Completable.complete();
//		return reply.content
//				.blockingForEach(e->{
//					
//				});
//				.ignoreElements()
//				.
//				.doOnComplete(()->System.err.println("Completed!"));
	}
	
	public static Flowable<byte[]> streamResponse(ContentChunk chunk) {
		
		return Flowable.generate((Emitter<byte[]> emitter) -> {
            ByteBuffer buffer = chunk.buffer;
            if (buffer.hasRemaining()) {
                emitter.onNext(getByteArrayFromByteBuffer(buffer));
            } else {
                chunk.callback.succeeded();
                emitter.onComplete();
            }
        });
	}

	public void close() throws Exception {
		this.httpClient.stop();
	}
	


    private static byte[] getByteArrayFromByteBuffer(ByteBuffer byteBuffer) {
        byte[] bytesArray = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytesArray, 0, bytesArray.length);
        return bytesArray;
    }

}
