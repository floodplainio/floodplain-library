package io.floodplain.integration

import io.floodplain.streams.debezium.JSONToReplicationMessage
import org.junit.Test
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Calendar

class TestDateParser {
    @Test
    fun testParse() {
        val valueInt = 4002000
                       val c = Calendar.getInstance();
               c.add(Calendar.DAY_OF_YEAR, valueInt);
        val ldt = LocalDateTime.ofEpochSecond(valueInt.toLong(), 0, ZoneOffset.UTC)
        println("Date: $ldt")
    }
}