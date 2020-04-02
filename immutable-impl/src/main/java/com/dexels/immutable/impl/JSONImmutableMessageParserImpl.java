package com.dexels.immutable.impl;


import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.immutable.json.ImmutableJSON;
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
