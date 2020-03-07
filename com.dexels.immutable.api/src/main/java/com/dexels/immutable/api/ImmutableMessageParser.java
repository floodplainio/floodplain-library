package com.dexels.immutable.api;

public interface ImmutableMessageParser {
	byte[] serialize(ImmutableMessage msg);
	String describe(ImmutableMessage msg);
}

