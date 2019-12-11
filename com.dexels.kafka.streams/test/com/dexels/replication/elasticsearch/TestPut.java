package com.dexels.replication.elasticsearch;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.http.HttpMethod;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dexels.http.reactive.elasticsearch.ElasticInsertTransformer;
import com.dexels.http.reactive.http.HttpInsertTransformer;
import com.dexels.http.reactive.http.JettyClient;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;

import io.reactivex.Flowable;

public class TestPut {
	
	@Before
	public void setup() {
		ReplicationFactory.setInstance(new JSONReplicationMessageParserImpl());
	}
	
	@Test @Ignore
	public void testFormat() throws IOException {
		ReplicationMessage r = ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json"));
		String res = new String(ImmutableFactory.ndJson(r.message()));
		
		System.err.println("result: "+res);
	}
	@Test @Ignore
	public void putSingleToElastic() throws Exception {
		String url = "http://elasticsearch:9200/_bulk";

		ReplicationMessage r = ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json"));
		String id = r.combinedKey();

		JettyClient myClient = new JettyClient();
		
		String result = myClient.callWithBody(url+"/doc/"+id, req->req.method(HttpMethod.POST).timeout(5, TimeUnit.SECONDS), Flowable.just(ImmutableFactory.ndJson(r.message()).getBytes()).doOnNext(e->System.err.println(">>> "+new String(e))), "application/x-ndjson")
				.doOnSuccess(e->System.err.println("Reply: "+e.status()))
				.toFlowable()
				.flatMap(e->e.content)
				.flatMap(c->JettyClient.streamResponse(c))
				.reduce(new StringBuilder(),(a,c)->a.append(new String(c)))
				.map(sb->sb.toString())
				.blockingGet();
		
	}
	
	@Test @Ignore
	public void putPrepToElastic() {
		ReplicationMessage r = ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json")).withSource(Optional.of("customertopic"));
//		ReplicationMessage r2 = new TopicElement("", "key2", ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json")));
		System.err.println("start1");
		Flowable.just(r)
				.doOnNext(e->System.err.println("First"))
			.compose(ElasticInsertTransformer.elasticSearchInserter((message)->message.source().orElse("ble"), (topic)->"sometype",100,100))
			.concatMap(e->e)
			.doOnNext(e->System.err.println("Data found"))
			.blockingForEach(e->System.err.println(new String(e)));
		
//		System.err.println("Reply: "+result);

	}
	
	@Test @Ignore
	public void testInsert() throws InterruptedException {
		String line1 = "{\"index\":{\"_index\":\"customertopic\",\"_type\":\"sometype\",\"_id\":\"596\"}}";
		String line2 = "{\"store_id\":1,\"last_update\":1369579785738000000,\"address_id\":602,\"last_name\":\"Forsythe\",\"active\":1,\"activebool\":true,\"customer_id\":596,\"create_date\":\"2006-02-14 01:00:00.00\",\"first_name\":\"Enrique\",\"email\":\"enrique.forsythe@sakilacustomer.org\",\"table\":\"customer\",\"Address\":{\"address\":\"1101 Bucuresti Boulevard\",\"address2\":\"\",\"phone\":\"199514580428\",\"district\":\"West Greece\",\"last_update\":1139996730000000000,\"address_id\":602,\"postal_code\":\"97661\",\"table\":\"address\",\"city_id\":401,\"City\":{\"city\":\"Patras\",\"last_update\":1139996725000000000,\"country_id\":39,\"table\":\"city\",\"city_id\":401,\"Country\":{\"country\":\"Greece\",\"last_update\":1139996640000000000,\"country_id\":39,\"table\":\"country\"}}}}";
		JettyClient cjc = new JettyClient();
		
		Flowable<byte[]> in = Flowable.just((line1+"\n"+line2+"\n").getBytes());
		Flowable.range(0, 5000)
				.flatMapSingle(e->cjc.call("http://elasticsearch:9200/_bulk",req->req.method(HttpMethod.POST)
					.timeout(5, TimeUnit.SECONDS),Optional.of(in), Optional.of("application/x-ndjson"),true),false,10)
//				.flatMapSingle(e->e)
				.doOnNext(e->System.err.println("Reply received"))
	        	.map(rep->JettyClient.ignoreReply(rep))
	        	.flatMapCompletable(e->e)
	        	.doOnComplete(()->System.err.println("completed"))
	        	.blockingAwait();
//				.compose(JettyClient.ignoreReply(reply))
//				.blockingForEach(e->System.err.println(new String(e)));
		
		System.err.println("waiting...");
		Thread.sleep(10000);
		System.err.println("Exiting");
		
	}

//	return inputStream.flatMap(e->client.callWithBody(url, buildRequest, Flowable.fromPublisher(e), contentType).toFlowable());

	@Test @Ignore
	public void putBulkToElastic2() {
		ReplicationMessage r = ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json")).withSource(Optional.of("customertopic"));
//		ReplicationMessage r2 = new TopicElement("", "key2", ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json")));
		System.err.println("start1");
		JettyClient cjc = new JettyClient();
		
//		Single<ReactiveReply> rr = cjc.call("http://localhost:9200/_bulk",req->req.method(HttpMethod.POST),Optional.of(item), Optional.of("application/x-ndjson"));
		
		String ress = Flowable.just(r)
				.doOnNext(e->System.err.println("First"))
			.compose(ElasticInsertTransformer.elasticSearchInserter((message)->message.source().orElse("ble"), (topic)->"sometype",100,100))
			.doOnNext(e->System.err.println("Data found"))
			.compose(cjc.call("http://elasticsearch:9200/_bulk", req->req.method(HttpMethod.POST),  Optional.of("application/x-ndjson"),1,1,true))
			.compose(JettyClient.responseStream())
			.reduce(new StringBuilder(),(a,c)->a.append(new String(c)))
			.map(sb->sb.toString())
			.blockingGet();
		System.err.println("Ress: "+ress);
	}

	
	@Test @Ignore
	public void putBulkToElastic() {
		ReplicationMessage r = ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json")).withSource(Optional.of("customertopic"));
//		ReplicationMessage r2 = new TopicElement("", "key2", ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json")));
		System.err.println("start1");
		String ress = Flowable.just(r)
				.doOnNext(e->System.err.println("First"))
			.compose(ElasticInsertTransformer.elasticSearchInserter((message)->message.source().orElse("ble"), (topic)->"sometype",100,100))
			.doOnNext(e->System.err.println("Data found"))
			.compose(HttpInsertTransformer.httpInsert("http://elasticsearch:9200/_bulk",req->req.method(HttpMethod.POST).timeout(2,TimeUnit.SECONDS), "application/x-ndjson",1,1,true))
			.compose(JettyClient.responseStream())
			.reduce(new StringBuilder(),(a,c)->a.append(new String(c)))
			.map(sb->sb.toString())
			.blockingGet();
		System.err.println("Ress: "+ress);
	}
}
