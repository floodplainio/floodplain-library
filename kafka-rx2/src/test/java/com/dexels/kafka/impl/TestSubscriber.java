package com.dexels.kafka.impl;

import com.dexels.kafka.factory.KafkaClientFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;
import io.reactivex.Flowable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class TestSubscriber {
    private KafkaTopicSubscriber kts;


    private final static Logger logger = LoggerFactory.getLogger(TestSubscriber.class);

    @Before
    public void setup() {
        Map<String, String> config = new HashMap<>();
//		config.put("wait", "5000");
//		config.put("max", "50");
        final String server = System.getenv("KAFKA_DEVELOP");
        logger.info("Connecting to server: {}", server);
        kts = (KafkaTopicSubscriber) KafkaClientFactory.createSubscriber(server, config);
    }

    @Test
    @Ignore
    public void testTopicList() {
        List<String> l = kts.topics();
        logger.info("Topics: >>>> {} <<<<", l.size());
        Assert.assertTrue("There should be topics", l.size() > 1);
    }

    @Test
    @Ignore
    public void testOffsetQuery() {
        Map<Integer, Long> l = kts.partitionOffsets("NAVAJO-WORKFLOW-EVENT-develop");
        logger.info(">>>> {} <<<<", l);

    }

    @Test
    @Ignore
    public void testTimestamps() {
        if (System.getenv("KAFKA_DEVELOP") == null) {
            return;
        }
        AtomicLong count = new AtomicLong();

        try {

            System.setProperty(ReplicationMessage.PRETTY_JSON, "true");

            ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);
            ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
//			AtomicLong requestCount = new AtomicLong();
            String topic = "testtopic";
            Flowable<byte[]> o = Flowable.fromPublisher(kts.subscribe(Arrays.asList(new String[]{topic}), "zzy", Optional.empty(), true, () -> {
            }))
                    .concatMapIterable(e -> e)
                    .doOnNext(e -> count.incrementAndGet())
                    .retry(5)
                    .map(e -> parser.parseBytes(Optional.of(topic), e.value()))
                    .map(e -> jsonparser.serialize(e));

            File dump = new File("dump.xml");
            FileOutputStream fos = new FileOutputStream(dump);
            o.blockingForEach(e -> {
                fos.write(e);
                count.incrementAndGet();
            });
            fos.close();

        } catch (Throwable e) {
            e.printStackTrace();
        }
        logger.info("I'm done!");
        logger.info("Count: " + count.get());


    }

    @Test(timeout = 10000)
    @Ignore
    public void testExample() {
        if (System.getenv("KAFKA_DEVELOP") == null) {
            return;
        }
        AtomicLong count = new AtomicLong();

        try {

            System.setProperty(ReplicationMessage.PRETTY_JSON, "true");

            ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);
            ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
//			AtomicLong requestCount = new AtomicLong();
//			StreamOper
            String topic = "testtopic-COMPETITIONTYPEWITHAGECLASS";
            String columns = "ageclasscode,ageclassname,competitionkind,competitiontypename,duration,extratime,gender,result_processing_method,isacategory,result_filter,series,sportid,sporttag,mandatorycoordinator,mandatoryassistants,mandatorysecretary,allowsassignmentbyclub,allowsmobiledwf,filloutbysecretary,fieldsize,availabledaysbefore,best_player_voting,mandatorygoaltakers";
            List<String> ll = Arrays.asList(columns.split(","));
            Flowable.fromPublisher(kts.subscribe(Arrays.asList(new String[]{topic}), UUID.randomUUID().toString(), Optional.empty(), true, () -> {
            }))
//					.filter(e->e.size()>0)
                    .map(e -> Flowable.fromIterable(e))
                    .concatMap(e -> e)
//					.concatMapIterable(e->e)
//					.doOnNext(e->count.incrementAndGet())
//					.retry(5)
                    .map(e -> parser.parseBytes(Optional.of(e.topic().orElse(topic)), e.value()))
                    .map(e -> e.withOnlyColumns(ll))
                    .map(e -> jsonparser.serialize(e))
                    .doOnError(e -> e.printStackTrace())
                    .blockingForEach(e -> {
                        logger.info("Item: " + new String(e));
                    });
        } catch (Throwable e) {
            e.printStackTrace();
        }
        logger.info("I'm done!");
        logger.info("Count: {}", count.get());


    }
}
