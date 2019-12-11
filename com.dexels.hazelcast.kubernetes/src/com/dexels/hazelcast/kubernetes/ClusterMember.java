package com.dexels.hazelcast.kubernetes;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ClusterMember {
	public String name();
	public Map<String,String> labels();
	public List<Integer> ports();
	public Optional<String> ip();
	public String namespace();
	public Optional<String> statusMessage();
	public String statusPhase();
	public Optional<String> parent();
}
