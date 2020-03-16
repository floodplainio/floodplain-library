package com.dexels.replication.api;

import com.dexels.immutable.api.ImmutableMessage;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface ReplicationMessage {
	
	public static final String KEYSEPARATOR = "<$>";
	public static final String PRETTY_JSON = "PRETTY_JSON";
	public static final String INCLUDE_KAFKA_METADATA = "INCLUDE_KAFKA_METADATA";
	public String transactionId();
	public Optional<String> source();
	public Optional<Integer> partition();
	public Optional<Long> offset();
	public long timestamp();
	public Operation operation();
	public List<String> primaryKeys();
	public Set<String> columnNames();
	public Object columnValue(String name);
	public String columnType(String name);
	public boolean equals(Object o);
	public enum Operation {
		INSERT, UPDATE, DELETE, NONE, COMMIT, MERGE, INITIAL
	}

	public String queueKey();
	public void commit();
	boolean isErrorMessage();
	public Map<String, Map<String,Object>> toDataMap();
	public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore);
	public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore, List<String> currentPath);
	public Map<String, Object> flatValueMap(boolean ignoreNull, Set<String> ignore, String prefix);
//	public Map<String, Object> flatValueMap(String prefix,Func3<String, String, Object, Object> processType);

	public boolean equalsToMessage(ReplicationMessage c);
	public boolean equalsByKey(ReplicationMessage c);
	public byte[] toBytes(ReplicationMessageParser c);
	public Map<String, String> types();
	
	public Optional<List<ImmutableMessage>> subMessages(String field);
	public Optional<ImmutableMessage> subMessage(String field);

	public ReplicationMessage withImmutableMessage(ImmutableMessage msg);

	public ReplicationMessage withSubMessages(String field, List<ImmutableMessage> message);
	public ReplicationMessage withSubMessage(String field, ImmutableMessage message);
	public ReplicationMessage withAddedSubMessage(String field, ImmutableMessage message);
	public ReplicationMessage withoutSubMessageInList(String field, Predicate<ImmutableMessage> s);
	public ReplicationMessage withoutSubMessages(String field);
	public ReplicationMessage withoutSubMessage(String field);
	public Set<String> subMessageListNames();
	public Set<String> subMessageNames();
	public ReplicationMessage without(String columnName);
	public ReplicationMessage without(List<String> columns);
	public ReplicationMessage with(String key, Object value, String type);
	public ReplicationMessage withOnlyColumns(List<String> columns);
	public ReplicationMessage withOnlySubMessages(List<String> subMessages);
	public ReplicationMessage rename(String columnName, String newName);
	public ReplicationMessage withPrimaryKeys(List<String> primary);
	public ReplicationMessage withSource(Optional<String> primary);
	public ReplicationMessage withPartition(Optional<Integer> partition);
	public ReplicationMessage withOffset(Optional<Long> offset);
	public ReplicationMessage now();
	public ReplicationMessage atTime(long timestamp);
	public String toFlatString(ReplicationMessageParser parser);

	public ReplicationMessage merge(ReplicationMessage other, Optional<List<String>> only);

	public static final boolean usePretty = System.getenv(PRETTY_JSON)!=null || System.getProperty(PRETTY_JSON)!=null;
	public static boolean usePrettyPrint() {
		return usePretty;
	}

//	private static final boolean includeKafkaMetadata = System.getenv(INCLUDE_KAFKA_METADATA)!=null || System.getProperty(INCLUDE_KAFKA_METADATA)!=null;
	public static boolean includeKafkaMetadata() {
		return false;
	}
		
	public Map<String, ImmutableMessage> subMessageMap();
	public Map<String, List<ImmutableMessage>> subMessageListMap();
	public ReplicationMessage withAllSubMessageLists(Map<String, List<ImmutableMessage>> subMessageListMap);
	public ReplicationMessage withAllSubMessage(Map<String, ImmutableMessage> subMessageMap);
	public ReplicationMessage withOperation(Operation operation);
	public Map<String, Object> values();
	public ReplicationMessage withCommitAction(Runnable commitAction);
	public ImmutableMessage message();
	public Optional<ImmutableMessage> paramMessage();
	public ReplicationMessage withParamMessage(ImmutableMessage msg);
	public ReplicationMessage withoutParamMessage();
	default public String combinedKey() {
		return primaryKeys().stream().map(k->columnValue(k).toString()).collect(Collectors.joining(KEYSEPARATOR));
	}
}
