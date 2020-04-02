package com.dexels.kafka.impl;

import com.dexels.kafka.factory.KafkaClientFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DescribeTopics {
    private KafkaTopicPublisher ktp;

    private static final Logger logger = LoggerFactory.getLogger(DescribeTopics.class);

    @Before
    public void setup() {
        final String server = System.getenv("KAFKA_DEVELOP");
        logger.info("Connecting to server: {}", server);
        ktp = (KafkaTopicPublisher) KafkaClientFactory.createPublisher("10.0.0.7:9092", 1, 1);
    }

    @Test
    @Ignore
    public void testDescribeTopic() {
        ktp.describeTopic("DVDSTORE-test-DVDS-address");
    }
}
