package com.dexels.kafka.streams.testdata;

import org.apache.commons.text.StrSubstitutor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TextTest {

    @Test
    public void test() {
        Map<String, String> valuesMap = new HashMap<String, String>();
        valuesMap.put("animal", "quick brown fox");
        valuesMap.put("target", "lazy dog");
        String templateString = "The ${animal} jumped over the ${target}.";
        StrSubstitutor sub = new StrSubstitutor(valuesMap);
        String resolvedString = sub.replace(templateString);
        System.err.println("resolved: " + resolvedString);
    }
}
