package io.floodplain.streams.api;

import java.util.Objects;

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

    public String toString() {
        return definition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProcessorName)) return false;
        ProcessorName that = (ProcessorName) o;
        return definition.equals(that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(definition);
    }
}
