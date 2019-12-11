package com.dexels.hazelcast.kubernetes;

import java.util.Map;

public interface Cluster {

	public String name();

	public Map<String, String> labels();

}
