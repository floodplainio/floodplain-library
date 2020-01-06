package com.dexels.immutable.api;

import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface ImmutableMessageParser {
	public byte[] serialize(ImmutableMessage msg);
	public String describe(ImmutableMessage msg);
}

