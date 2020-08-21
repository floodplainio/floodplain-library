package io.floodplain.streams.testdata;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.debezium.DebeziumParseException;
import io.floodplain.streams.debezium.JSONToReplicationMessage;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class TestDeserializeDelete {

    private final static Logger logger = LoggerFactory.getLogger(TestDeserializeDelete.class);

    @Test
    public void testDeleteFilmActorExample() throws IOException, DebeziumParseException {
        InputStream is = getClass().getClassLoader().getResourceAsStream("delete_film_actor.json");
        Assert.assertNotNull("Missing resource",is);
        ReplicationMessage rm = JSONToReplicationMessage.processDebeziumBody(is.readAllBytes(), Optional.of("buba"));
        logger.info("Rm: {}",rm);
    }
}
