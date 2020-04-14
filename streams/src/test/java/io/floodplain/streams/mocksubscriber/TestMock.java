package io.floodplain.streams.mocksubscriber;

import io.floodplain.http.reactive.graph.NeoInsertTransformer;
import io.floodplain.http.reactive.http.JettyClient;
import io.floodplain.pubsub.rx2.factory.PubSubTools;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import io.reactivex.Flowable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStreamReader;
import java.util.Optional;

public class TestMock {

    @Test
    public void testSimpleMock() {
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

        long lines = Flowable.fromPublisher(PubSubTools.createMockSubscriber(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("customerslice.dump"))).subscribe("any", "any", true))
                .concatMap(e -> Flowable.fromIterable(e))
                .map(e -> ReplicationFactory.getInstance().parseBytes(Optional.of("customer"), e.value()))
                .map(e -> e.combinedKey())
                .groupBy(e -> e)
                .doOnNext(e -> System.err.println(">> " + e))
                .count().blockingGet();

        Assert.assertEquals(5, lines);
    }

    @Test
    public void testBufferMock() {
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

        long lines = Flowable.fromPublisher(PubSubTools.createMockSubscriber(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("customerslice.dump"))).subscribe("any", "any", true))
                .concatMap(e -> Flowable.fromIterable(e))
                .map(e -> ReplicationFactory.getInstance().parseBytes(Optional.of("customer"), e.value()))
                .map(e -> e.combinedKey())
                .buffer(2)
                .doOnNext(e -> System.err.println(">> " + e))
                .count().blockingGet();

        Assert.assertEquals(3, lines);
    }

    @Test
    @Ignore
    public void testMockActor() {
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

        long lines = Flowable.fromPublisher(PubSubTools.createMockSubscriber(new InputStreamReader(getClass().getResourceAsStream("actor"))).subscribe("any", "any", true))
                .concatMap(e -> Flowable.fromIterable(e))
                .map(e -> ReplicationFactory.getInstance().parseBytes(Optional.of("actor"), e.value()))
                .buffer(2)
                .compose(NeoInsertTransformer.neoNodeTransformer("ACTOR"))
                .compose(NeoInsertTransformer.neoInserter("http://neo4j:7474/db/data/transaction/commit", "neo4j", "neo4jpw"))
//				.reduce(new ByteArrayOutputStream(),(baos,bytes)->baos.)
//				.concatMap(e->e)
                .compose(JettyClient.responseStream())
                .doOnNext(e -> System.err.println(">> " + new String(e)))
                .count().blockingGet();

        Assert.assertEquals(100, lines);
    }

    @Test
    @Ignore
    public void testMockFilm() {
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

        long lines = Flowable.fromPublisher(PubSubTools.createMockSubscriber(new InputStreamReader(getClass().getResourceAsStream("film"))).subscribe("any", "any", true))
                .concatMap(e -> Flowable.fromIterable(e))
                .map(e -> ReplicationFactory.getInstance().parseBytes(Optional.of("film"), e.value()))
                .buffer(2)
                .compose(NeoInsertTransformer.neoNodeTransformer("FILM"))
                .compose(NeoInsertTransformer.neoInserter("http://neo4j:7474/db/data/transaction/commit", "neo4j", "neo4jpw"))
//				.reduce(new ByteArrayOutputStream(),(baos,bytes)->baos.)
//				.concatMap(e->e)
                .compose(JettyClient.responseStream())
                .doOnNext(e -> System.err.println(">> " + new String(e)))
                .count().blockingGet();

        Assert.assertEquals(500, lines);
    }

    @Test
    @Ignore
    public void testMockPlaysIn() {
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

        long lines = Flowable.fromPublisher(PubSubTools.createMockSubscriber(new InputStreamReader(getClass().getResourceAsStream("film_actor"))).subscribe("any", "any", true))
                .concatMap(e -> Flowable.fromIterable(e))
                .map(e -> ReplicationFactory.getInstance().parseBytes(Optional.of("actor"), e.value()))
                .buffer(2)
                .compose(NeoInsertTransformer.neoRelationshipTransformer("FILM"))
                .compose(NeoInsertTransformer.neoInserter("http://neo4j:7474/db/data/transaction/commit", "neo4j", "neo4jpw"))
//				.reduce(new ByteArrayOutputStream(),(baos,bytes)->baos.)
//				.concatMap(e->e)
                .compose(JettyClient.responseStream())
                .doOnNext(e -> System.err.println(">> " + new String(e)))
                .count().blockingGet();

        Assert.assertEquals(2731, lines);
    }

    @Test
    @Ignore
    public void testMock() {
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

        long lines = Flowable.fromPublisher(PubSubTools.createMockSubscriber(new InputStreamReader(getClass().getResourceAsStream("customerslice.dump"))).subscribe("any", "any", true))
                .concatMap(e -> Flowable.fromIterable(e))
                .map(e -> ReplicationFactory.getInstance().parseBytes(Optional.of("customer"), e.value()))
                .buffer(2)
                .compose(NeoInsertTransformer.neoNodeTransformer("customer"))
                .compose(NeoInsertTransformer.neoInserter("http://neo4j:7474/db/data/transaction/commit", "neo4j", "neo4jpw"))
//				.reduce(new ByteArrayOutputStream(),(baos,bytes)->baos.)
//				.concatMap(e->e)
                .compose(JettyClient.responseStream())
                .doOnNext(e -> System.err.println(">> " + new String(e)))
                .count().blockingGet();

        Assert.assertEquals(3, lines);
    }
}
