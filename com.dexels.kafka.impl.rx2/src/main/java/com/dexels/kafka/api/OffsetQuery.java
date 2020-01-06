package com.dexels.kafka.api;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;


public interface OffsetQuery {
	public List<String> topics();
	public Map<Integer,Long> partitionOffsets(String topic);
	public Map<String,Map<Integer,Long>> offsets(List<String> topics);
	
	// Tag encoding:
	public String encodeTag(Map<String, Map<Integer, Long>> tag);
	public BiFunction<String, Integer, Long> decodeTag(String tag);
	public String encodeTopicTag(Function<Integer, Long> tag, List<Integer> partitions);
	public Function<Integer, Long> decodeTopicTag(String tag);
}
