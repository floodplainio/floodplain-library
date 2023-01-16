package io.floodplain.streams.testdata;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.DateSerializer;
import io.floodplain.streams.debezium.DebeziumParseException;
import io.floodplain.streams.debezium.JSONToReplicationMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.Optional;

public class TestDateParsing {
    private final static Logger logger = LoggerFactory.getLogger(TestDateParsing.class);

    @Test
    public void testTimestamp() throws IOException, DebeziumParseException {
        InputStream is = getClass().getClassLoader().getResourceAsStream("timeslot.json");
        Assertions.assertNotNull(is,"Missing resource");
        ReplicationMessage rm = JSONToReplicationMessage.processDebeziumBody(is.readAllBytes(), Optional.of("sometable"));
        ImmutableMessage im = rm.message();
        Instant o = (Instant) im.value("date_created").get();
        Long l = o.toEpochMilli();
    Assertions.assertEquals(1666189812261L,l);
    }
    @Test
    public void testParse() {
        String val = "2022-10-19T14:30:12.261837Z";
        Temporal a = DateSerializer.parseTimeObject(val);
        logger.info("Val: {}",a);
    }

}
