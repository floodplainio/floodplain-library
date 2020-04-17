package io.floodplain.streams.testdata;

import io.floodplain.streams.api.CoreOperators;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestUngroupKey {

    private final static Logger logger = LoggerFactory.getLogger(TestUngroupKey.class);


    @Test
    public void test() {
        String key = "aap|noot";
        final String ungrouped = CoreOperators.ungroupKey(key);
        Assert.assertEquals("noot", ungrouped);
        logger.info(ungrouped);
    }

    @Test
    public void testTriple() {
        String key = "aap|noot|mies";
        final String ungrouped = CoreOperators.ungroupKey(key);
        Assert.assertEquals("mies", ungrouped);
        logger.info(ungrouped);
    }

}
