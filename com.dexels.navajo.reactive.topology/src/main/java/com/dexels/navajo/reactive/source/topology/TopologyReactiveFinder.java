package com.dexels.navajo.reactive.source.topology;

import com.dexels.navajo.reactive.CoreReactiveFinder;

public class TopologyReactiveFinder extends CoreReactiveFinder {

	public TopologyReactiveFinder() {
		addReactiveSourceFactory(new TopicSourceFactory(),"topic");
		addReactiveSourceFactory(new DebeziumSourceFactory(),"debezium");
		addReactiveTransformerFactory(new GroupTransformerFactory(),"group");
		addReactiveTransformerFactory(new SinkTransformerFactory(),"sink");
		addReactiveTransformerFactory(new FilterTransformerFactory(),"filter");
		addReactiveTransformerFactory(new JoinWithTransformerFactory(),"joinWith");
		addReactiveTransformerFactory(new ScanToListTransformerFactory(),"scanToList");
		reactiveReducer.put("set", new Set());
	}

}
