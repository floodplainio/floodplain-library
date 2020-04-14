package io.floodplain.immutable.impl;


import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessageParser;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.immutable.json.ImmutableJSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONImmutableMessageParserImpl implements ImmutableMessageParser {

    private static final boolean INCLUDENULLVALUES = true;

    private final static Logger logger = LoggerFactory.getLogger(JSONImmutableMessageParserImpl.class);

    @Override
    public byte[] serialize(ImmutableMessage msg) {
        return ImmutableJSON.jsonSerializer(msg, INCLUDENULLVALUES, true);
    }

    @Override
    public String describe(ImmutableMessage msg) {
        return new String(ImmutableJSON.jsonSerializer(msg, INCLUDENULLVALUES, false));
    }

    public void activate() {
        logger.info("Immutable parser constructed");
//		logger.
        ImmutableFactory.setInstance(this);
    }

    public void deactivate() {
        ImmutableFactory.setInstance(null);
    }

}
