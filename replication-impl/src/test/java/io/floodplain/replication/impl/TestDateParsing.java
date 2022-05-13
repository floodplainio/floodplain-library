package io.floodplain.replication.impl;

import io.floodplain.replication.factory.DateSerializer;
import org.junit.jupiter.api.Test;

import java.time.temporal.Temporal;

public class TestDateParsing {

    @Test
    public void testDateSerialization() {
        //+17519-05-05 16:00:00.000000
        Temporal t = DateSerializer.parseTimeObject("+17519-05-05 16:00:00.000000");

    }
}
