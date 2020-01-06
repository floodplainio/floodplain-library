package com.dexels.http.reactive.test;

import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.http.HttpMethod;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dexels.http.reactive.http.HttpInsertTransformer;
import com.dexels.http.reactive.http.JettyClient;
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
	public void putBulkToElastic() {
		final InputStream resourceAsStream = getClass().getResourceAsStream("customer.json");
		ReplicationMessage r = ReplicationFactory.getInstance().parseStream(resourceAsStream).withSource(Optional.of("customertopic"));
//		ReplicationMessage r2 = new TopicElement("", "key2", ReplicationFactory.getInstance().parseStream(getClass().getClassLoader().getResourceAsStream("customer.json")));
		Flowable<byte[]> in = Flowable.just(ReplicationFactory.getInstance().serialize(r));

		String ress = Flowable.just(in)
				.doOnNext(e->System.err.println("First"))
			.compose(HttpInsertTransformer.httpInsert("http://localhost:1111/_bulk",req->req.method(HttpMethod.POST).timeout(2,TimeUnit.SECONDS), "application/x-ndjson",1,1,true))
			.compose(JettyClient.responseStream())
			.reduce(new StringBuilder(),(a,c)->a.append(new String(c)))
			.doOnError(e->{
				System.err.println("detected");
				e.printStackTrace();
				
			})
			.map(sb->sb.toString())
			.blockingGet();
		System.err.println("Ress: "+ress);
	}
}
