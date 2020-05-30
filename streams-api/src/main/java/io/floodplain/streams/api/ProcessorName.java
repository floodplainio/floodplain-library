package io.floodplain.streams.api;

public class ProcessorName {
    private final String definition;

    public static ProcessorName from(String definition) {
        return new ProcessorName(definition);
    }
    private ProcessorName(String definition) {
        this.definition = definition;
    }

    public String definition() {
        return definition;
    }
}
