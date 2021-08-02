package io.floodplain.replication.impl.json.test;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class TestJsonTypes {
    private static final Logger logger = LoggerFactory.getLogger(TestJsonTypes.class);
    private ReplicationMessage organization;

    @Test
    public void test() {
        System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
        ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
        ReplicationFactory.setInstance(parser);
        organization = parser.parseStream(TestJoin.class.getClassLoader().getResourceAsStream("organization.json"));
        Optional<Object> isValid = organization.value("isValid");
        Object a = isValid.get();
    }
}
