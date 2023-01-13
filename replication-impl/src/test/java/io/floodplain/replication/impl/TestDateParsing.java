package io.floodplain.replication.impl;

import io.floodplain.replication.factory.DateSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;

public class TestDateParsing {

    private static final Logger logger = LoggerFactory.getLogger(TestDateParsing.class);

    @Test
    public void testDateSerialization() {
        Temporal t = DateSerializer.parseTimeObject("+17519-05-05 16:00:00.000000");
        Assertions.assertNull(t);
    }
    @Test
    public void testZoned() {
        ZoneId zoneId = ZoneId.of("UTC+1");
        ZonedDateTime zdt = ZonedDateTime.of(2022, 1, 30, 23, 45, 59, 1234, zoneId);
        String result = DateSerializer.serializeTimeObject(zdt);
        System.err.println("Create zoned: "+result);
        logger.info("Create zoned: "+result);
        Assertions.assertEquals("2022-01-30 23:45:59.000+01:00",result);
    }

    @Test
    public void testZonedZulu() {
        Temporal t = DateSerializer.parseTimeObject("2022-10-19T14:30:12.261837Z");
        Assertions.assertNotNull(t);

    }
}
