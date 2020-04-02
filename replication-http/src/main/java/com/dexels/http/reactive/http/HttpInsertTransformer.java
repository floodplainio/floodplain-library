package com.dexels.http.reactive.http;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.eclipse.jetty.client.api.Request;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;

public class HttpInsertTransformer implements FlowableTransformer<Flowable<byte[]>, ReactiveReply> {

    private static final JettyClient client = new JettyClient();
    private final String url;
    private final Function<Request, Request> buildRequest;
    private final String contentType;
    private final boolean logHttp;
//	private final int maxConcurrent;
//	private final int prefetch;

    private final static Logger logger = LoggerFactory.getLogger(HttpInsertTransformer.class);


    private HttpInsertTransformer(String url, Function<Request, Request> buildRequest, String contentType, int maxConcurrent, int prefetch, boolean logHttp) {
        this.url = url;
        this.buildRequest = buildRequest;
        this.contentType = contentType;
        this.logHttp = logHttp;
//		this.maxConcurrent = maxConcurrent;
//		this.prefetch = prefetch;
    }

    public static FlowableTransformer<Flowable<byte[]>, ReactiveReply> httpInsert(String url, Function<Request, Request> buildRequest, String contentType, int maxConcurrent, int prefetch, boolean logHttp) {
        return client.call(url, buildRequest, Optional.of(contentType), maxConcurrent, prefetch, logHttp);
    }

//	.compose(cjc.call("http://localhost:9200/_bulk", req->req.method(HttpMethod.POST),  Optional.of("application/x-ndjson")))


    private Flowable<ReactiveReply> callHttp(Flowable<Flowable<byte[]>> inputStream) {
//		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (this.logHttp) {
            inputStream = inputStream.doOnSubscribe(e -> logger.info("Started request stream to url: {}", url))
                    .doOnRequest(i -> System.err.println("Data requested: " + i));
        }
        return inputStream.flatMap(e -> client.callWithBody(url, buildRequest, e, contentType).toFlowable())
                .doOnError(e -> {
                    logger.error("Failed: ", e);
//					if(e instanceof ReactiveException) {
//						ReactiveException re = (ReactiveException)e;
//						System.err.println("Request: "+re.request);
//					}
                });
    }

    @Override
    public Publisher<ReactiveReply> apply(Flowable<Flowable<byte[]>> in) {
        return e -> callHttp(in);
    }
}
