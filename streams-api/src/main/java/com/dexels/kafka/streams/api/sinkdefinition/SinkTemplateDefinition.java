package com.dexels.kafka.streams.api.sinkdefinition;

import java.util.Map;

public class SinkTemplateDefinition {

    private String name;
    private Map<String, Object> settings;

    public String getName() {
        return name;
    }

    public Map<String, Object> getSettings() {
        return this.settings;
    }

    public void activate(Map<String, Object> settings) {
        name = (String) settings.get("name");
        this.settings = settings;
    }
}
