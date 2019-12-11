package com.dexels.hazelcast.kubernetes;

import java.util.List;
import java.util.Map;

public interface StatefulSet {

	public String name();

	public Map<String, String> labels();

    public Map<String, String> status();

    public Map<String, String> templateLabels();

    public List<Event> events();

    public Map<String, String> eventsMap();

}
