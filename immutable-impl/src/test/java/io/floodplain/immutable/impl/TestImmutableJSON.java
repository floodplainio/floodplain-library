package io.floodplain.immutable.impl;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessageParser;
import io.floodplain.immutable.factory.ImmutableFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static io.floodplain.immutable.api.ImmutableMessage.ValueType.*;

public class TestImmutableJSON {

    private ImmutableMessageParser parser;

    private final static Logger logger = LoggerFactory.getLogger(TestImmutableJSON.class);

    @Before
    public void setup() {
        parser = new JSONImmutableMessageParserImpl();
    }

    @Test
    public void testImmutable() {
        ImmutableMessage msg = ImmutableFactory.empty().with("teststring", "bla", STRING)
                .with("testinteger", 3, INTEGER);
        byte[] bytes = parser.serialize(msg);
        logger.info("TEST: {}", new String(bytes));
    }

    @Test
    public void testDescribe() {
        ImmutableMessage msg = ImmutableFactory.empty().with("teststring", "bla", STRING)
                .with("testinteger", 3, INTEGER);
        String description = parser.describe(msg);
        logger.info("DESCRIPTION: {}", description);
    }

    @Test
    public void testAddSubMessage() {
        ImmutableMessage empty = ImmutableFactory.empty();
        ImmutableMessage created = empty.with("Aap/Noot", 3, INTEGER);
        Optional<ImmutableMessage> sub = created.subMessage("Aap");
        Assert.assertTrue(sub.isPresent());
        Assert.assertEquals(3, sub.get().value("Noot").get());
    }

    @Test
    public void testGetSubValue() {
        ImmutableMessage empty = ImmutableFactory.empty();
        ImmutableMessage created = empty.with("Aap/Noot", 3, INTEGER);
        Assert.assertEquals(3, created.value("Aap/Noot").get());
    }

    @Test
    public void testSubMessageUsingWith() {
        ImmutableMessage created = ImmutableFactory.empty().with("Aap", 3, INTEGER);
        ImmutableMessage someOther = ImmutableFactory.empty().with("Noot", 4, INTEGER);
        ImmutableMessage combined = created.with("submessage", someOther, IMMUTABLE);
        Assert.assertEquals(4, combined.value("submessage/Noot").get());
    }


    @Test
    public void testNdJSON() throws IOException {
        ImmutableMessage m = ImmutableFactory.empty().with("somenumber", 3, INTEGER);
        logger.info("{}", ImmutableFactory.ndJson(m));

    }
}
