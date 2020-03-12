package com.dexels.navajo.reactive.source.topology;

import com.dexels.navajo.reactive.CoreReactiveFinder;

public class TopologyReactiveFinder extends CoreReactiveFinder {

	public TopologyReactiveFinder() {
		addReactiveSourceFactory(new TopicSourceFactory(),"topic");
		addReactiveSourceFactory(new DebeziumTopicFactory(),"database");
		addReactiveTransformerFactory(new GroupTransformerFactory(),"group");
		addReactiveTransformerFactory(new SinkTransformerFactory(),"sink");
		addReactiveTransformerFactory(new FilterTransformerFactory(),"filter");
		addReactiveTransformerFactory(new JoinWithTransformerFactory(),"joinWith");
		addReactiveTransformerFactory(new JoinRemoteTransformerFactory(),"joinRemote");
		addReactiveTransformerFactory(new ScanToListTransformerFactory(),"scanToList");
		addReactiveTransformerFactory(new ScanTransformerFactory(),"scan");
		addReactiveTransformerFactory(new SetFactory(),"set");
		addReactiveTransformerFactory(new OnlyFactory(),"only");
		addReactiveTransformerFactory(new LogTransformerFactory(),"log");
		addReactiveTransformerFactory(new RowNumberTransformerFactory(),"rownum");
	}
}
