package com.dexels.kafka.streams.api.sink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.dexels.replication.api.ReplicationMessage;

import io.reactivex.functions.Action;

public class SinkTuple<T> {
	public final List<ReplicationMessage> in;
	public final List<T> models;
	public final Action commit;
	
	
	public SinkTuple(List<ReplicationMessage> in, Function<? super ReplicationMessage, T> builder) {
		Map<String,Long> offsetMap = new HashMap<>();
		Map<String,ReplicationMessage> toCommit = new HashMap<>();
		in.stream().filter(e->e.source().isPresent() && e.partition().isPresent() && e.offset().isPresent()).forEach(msg->{
			String path = msg.source().get()+"/"+msg.partition().get();
			long offset = msg.offset().get();
			Long existing = offsetMap.get(path);
			if(existing==null || offset > existing) {
				offsetMap.put(path, offset);
				toCommit.put(path, msg);
			}
		});
		System.err.println("Offset: "+offsetMap);
		this.models = in.stream().map(e->builder.apply(e)).collect(Collectors.toList());
		this.commit = ()->{
			toCommit.values().forEach(e->{
				e.commit();
			});
		};
		this.in = in;
	}
}
