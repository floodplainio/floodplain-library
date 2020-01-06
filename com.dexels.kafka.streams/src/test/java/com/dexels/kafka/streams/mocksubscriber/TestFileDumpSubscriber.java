package com.dexels.kafka.streams.mocksubscriber;

import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import org.junit.Assert;
import org.junit.Test;

import com.dexels.pubsub.rx2.factory.PubSubTools;

import io.reactivex.Flowable;

public class TestFileDumpSubscriber {

	@Test 
	public void testFile() throws FileNotFoundException  {
		long lines = Flowable.fromPublisher(PubSubTools.createMockSubscriber(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("customerslice.dump"))).subscribe("any", "any", true))
			.concatMap(e->Flowable.fromIterable(e))
			.count().blockingGet();
		
		Assert.assertEquals(5, lines);
		

	}

}
