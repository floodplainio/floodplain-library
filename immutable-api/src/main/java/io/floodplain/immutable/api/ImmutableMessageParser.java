package io.floodplain.immutable.api;

public interface ImmutableMessageParser {
    byte[] serialize(ImmutableMessage msg);

    String describe(ImmutableMessage msg);
}

