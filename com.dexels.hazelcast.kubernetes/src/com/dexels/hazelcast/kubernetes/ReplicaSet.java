package com.dexels.hazelcast.kubernetes;

import java.util.Map;
import java.util.Optional;

public interface ReplicaSet {
	public String name();
	public Map<String, String> labels();
	public Optional<String> parent();
}
      