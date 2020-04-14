package io.floodplain.pubsub.rx2.factory.impl.internal.test;

import io.floodplain.pubsub.rx2.factory.PubSubTools;
import io.floodplain.pubsub.rx2.factory.impl.internal.KafkaDumpSubscriber;
import io.reactivex.Flowable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.InputStreamReader;

public class TestFileDumpSubscriber {

    @Test
    @Ignore
    public void testFileDirect() throws FileNotFoundException {
        KafkaDumpSubscriber ts = new KafkaDumpSubscriber(new InputStreamReader(getClass().getResourceAsStream("customerslice.dump")));
        ts.readLines()
                .map(ts::parseLine)
                .subscribe(e -> System.err.println("Message: >" + e.key() + "<"));

    }


    @Test
    @Ignore
    public void testFile() throws FileNotFoundException {
        long lines = Flowable.fromPublisher(PubSubTools.createMockSubscriber(new InputStreamReader(getClass().getResourceAsStream("customerslice.dump"))).subscribe("any", "any", true))
                .concatMap(e -> Flowable.fromIterable(e))
                .count().blockingGet();

        Assert.assertEquals(5, lines);
    }

}
