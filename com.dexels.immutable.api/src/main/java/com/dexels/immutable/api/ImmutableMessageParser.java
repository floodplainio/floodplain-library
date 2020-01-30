package com.dexels.immutable.api;

public interface ImmutableMessageParser {
	public byte[] serialize(ImmutableMessage msg);
	public String describe(ImmutableMessage msg);
}

