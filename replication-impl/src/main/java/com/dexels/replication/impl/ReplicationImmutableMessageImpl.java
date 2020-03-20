package com.dexels.replication.impl;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.Trifunction;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.dexels.immutable.api.ImmutableMessage.*;

public class ReplicationImmutableMessageImpl implements ReplicationMessage {

	private final static Logger logger = LoggerFactory.getLogger(ReplicationImmutableMessageImpl.class);
	private final Optional<String> source;
	private final String transactionId;
	private final long timestamp;
	private final Operation operation;
	private final List<String> primaryKeys;
	private Optional<Runnable> commitAction;
	private final Optional<Integer> partition;
	private final Optional<Long> offset;

	
	
	private final ImmutableMessage immutableMessage;
	private final Optional<ImmutableMessage> paramMessage;



	public ReplicationImmutableMessageImpl(Optional<String> source, Optional<Integer> partition, Optional<Long> offset, String transactionId, Operation operation, long timestamp, Map<String,Object> values, Map<String, ValueType> types, Map<String,ImmutableMessage> submessage, Map<String,List<ImmutableMessage>> submessages, List<String> primaryKeys, Optional<Runnable> commitAction, Optional<ImmutableMessage> paramMessage) {
		this.immutableMessage = ImmutableFactory.create(values, types, submessage, submessages);
		this.transactionId = transactionId;
		this.timestamp = timestamp;
		this.operation = operation;
		this.primaryKeys = Collections.unmodifiableList(primaryKeys);
		this.commitAction = commitAction;
		this.source = source;
		this.partition = partition;
		this.offset = offset;
		this.paramMessage = paramMessage;
	}

	public ReplicationImmutableMessageImpl(Optional<String> source, Optional<Integer> partition, Optional<Long> offset, String transactionId, Operation operation, long timestamp, ImmutableMessage parentMessage , List<String> primaryKeys, Optional<Runnable> commitAction, Optional<ImmutableMessage> paramMessage) {
		this.immutableMessage = parentMessage;
		this.transactionId = transactionId;
		this.timestamp = timestamp;
		this.operation = operation;
		this.primaryKeys = Collections.unmodifiableList(primaryKeys);
		this.commitAction = commitAction;
		this.source = source;
		this.offset = offset;
		this.partition = partition;
		this.paramMessage = paramMessage;
	}

	public ReplicationImmutableMessageImpl(ReplicationMessage message1, ReplicationMessage message2, String key) {
		this.transactionId = null; // message1.transactionId()+"-"+message2.transactionId();
		this.timestamp = System.currentTimeMillis();
		this.operation = Operation.MERGE;
		this.primaryKeys = Collections.unmodifiableList(Arrays.asList(new String[]{key}));
		this.immutableMessage = message1.message().merge(message2.message(), Optional.empty());
		this.source = Optional.empty();
		this.partition = Optional.empty();
		this.offset = Optional.empty();
		this.paramMessage = Optional.empty();

	}
	
	@Override
	public ImmutableMessage message() {
		return immutableMessage;
	}
	@Override
	public Set<String> subMessageNames() {
		return message().subMessageNames();
	}
	
	@Override
	public Set<String> subMessageListNames() {
		return message().subMessageListNames();
	}

	@Override
	public String queueKey() {
		if(primaryKeys.size()==0) {
			return "NO_KEY_PRESENT";
		}
		return String.join(ReplicationMessage.KEYSEPARATOR, primaryKeys.stream().map(col->""+columnValue(col)).collect(Collectors.toList()));
	}
	
	@Override
	public byte[] toBytes(ReplicationMessageParser c) {
		return c.serialize(this);
	}
	public boolean equals(Object other) {
		if(!(other instanceof ReplicationMessage)) {
			return false;
		}
		return equalsByKey((ReplicationMessage)other);
	}
	
	
	public boolean equalsByKey(ReplicationMessage other) {
		String key = queueKey();
		if(key==null) {
			return super.equals(other);
		}
		return key.equals(other.queueKey());
//		return key.equals(((ReplicationImmutableMessageImpl)other).queueKey());
	}

	public int hashCode() {
		String key = queueKey();
		if(key==null) {
			return super.hashCode();
		}
		return key.hashCode();
	}

	public ReplicationImmutableMessageImpl(Throwable t) {
		logger.error("Creating failure replication message",t);
		t.printStackTrace(System.err);
		t.printStackTrace(System.out);
		this.transactionId = null;
		this.timestamp = -1;
//		this.status = null;
		this.operation = Operation.UPDATE;
		this.primaryKeys = Collections.emptyList();
		this.immutableMessage = ImmutableFactory.create(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
		this.source = Optional.empty();
		this.partition = Optional.empty();
		this.offset = Optional.empty();
		this.paramMessage = Optional.empty();
	}
	
	@SuppressWarnings("unchecked")
	public ReplicationImmutableMessageImpl(Map<String,Object> initial) {
		this.transactionId = (String) initial.get("TransactionId");
		this.timestamp = (long) initial.get("Timestamp");
		this.operation = Operation.valueOf((String) initial.get("Operation"));
		this.primaryKeys = Collections.unmodifiableList(((List<String>) initial.get("PrimaryKeys")));
		Map<String,Object> initialValues = Collections.unmodifiableMap((Map<? extends String, ? extends Object>) initial.get("Columns"));
		this.immutableMessage = ImmutableFactory.create(initialValues, resolveTypesFromValues(initialValues) , Collections.emptyMap(), Collections.emptyMap());
		this.source = Optional.empty();
		this.partition = Optional.empty();
		this.offset = Optional.empty();
		this.paramMessage = Optional.empty();
		
	}



	private Map<String, ValueType> resolveTypesFromValues(Map<String, Object> values) {
		Map<String, ValueType> t = new HashMap<>();
		for (Entry<String,Object> e : values.entrySet()) {
			Object val = e.getValue();
			if(val==null) {
//				t.put(e.getKey(), "unknown");
			} else if(val instanceof Long) {
				t.put(e.getKey(), ValueType.LONG);
			} else if(val instanceof Double) {
				t.put(e.getKey(), ValueType.DOUBLE);
			} else if(val instanceof Integer) {
				t.put(e.getKey(), ValueType.INTEGER);
			} else if(val instanceof Float) {
				t.put(e.getKey(), ValueType.FLOAT);
			} else if(val instanceof Date) {
				t.put(e.getKey(), ValueType.DATE);
			} else if(val instanceof Boolean) {
				t.put(e.getKey(), ValueType.BOOLEAN);
			} else if(val instanceof String) {
				t.put(e.getKey(), ValueType.STRING);
			} else {
				logger.warn("Unknown type::: {}",val.getClass());
				t.put(e.getKey(),ValueType.STRING);
				
			}
		}
		return t;
	}

//
//	private Map<String, String> combineTypes(Map<String, String> typesa, Map<String, String> typesb) {
//		HashMap<String,String> combine = new HashMap<>(typesa);
//		for (Entry<String,String> e : typesb.entrySet()) {
//			// TODO sanity check types?
//			combine.put(e.getKey(), e.getValue());
//		}
//		return Collections.unmodifiableMap(combine);
//	}

	@Override
	public Map<String,ValueType> types() {
		return message().types();
	}
 	
	@Override
	public Map<String, Map<String,Object>> toDataMap() {
		return message().toDataMap();
	}
	
	@Override
	public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore) {
		return valueMap(ignoreNull,ignore,Collections.emptyList());
	}
//	
//	private static BiFunction<String,List<String>,Boolean> checkIgnoreList(Set<String> ignoreList) {
//		return (item,currentPath)->{
//			if(ignoreList.contains(item)) {
//				return false;
//			}
//			return true;
//		};
//	}
	
	@Override
	public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore, List<String> currentPath) {
		return message().valueMap(ignoreNull, ignore,currentPath);
	}
	@Override
	public boolean isErrorMessage() {
		return transactionId==null;
	}

	@Override
	public String transactionId() {
		return transactionId;
	}

	@Override
	public long timestamp() {
		return timestamp;
	}

	@Override
	public Operation operation() {
		return operation;
	}

	@Override
	public List<String> primaryKeys() {
		return this.primaryKeys;
	}

	@Override
	public Set<String> columnNames() {
		return message().columnNames();
	}

	@Override
	public Object columnValue(String name) {
		return message().columnValue(name);
	}

	@Override
	public ValueType columnType(String name) {
		return message().columnType(name);
	}

	@Override
	public String toString() {
		return "Operation: " + this.operation.toString() +  " Ts: " + this.timestamp + "Transactionid: "+this.transactionId+" pk: "+primaryKeys+"Value:\n" +message().toString();
	}

	@Override
	public void commit() {
		if(this.commitAction!=null && this.commitAction.isPresent()) {
			this.commitAction.get().run();
		}
	}

	@Override
	public Optional<List<ImmutableMessage>> subMessages(String field) {
		return message().subMessages(field);
	}

	
	@Override
	public Optional<ImmutableMessage> subMessage(String field) {
		return message().subMessage(field);
	}

	@Override
	public ReplicationMessage withImmutableMessage(ImmutableMessage msg) {
		return new ReplicationImmutableMessageImpl(this.source, this.partition,this.offset, this.transactionId,this.operation,this.timestamp, msg , primaryKeys,this.commitAction,this.paramMessage);
	}
	@Override
	public ReplicationMessage withSubMessages(String field, List<ImmutableMessage> message) {
		return withImmutableMessage(message().withSubMessages(field, message));
	}

	@Override
	public ReplicationMessage withSubMessage(String field, ImmutableMessage message) {
		return withImmutableMessage(message().withSubMessage(field, message));
	}

	@Override
	public ReplicationMessage without(String columnName) {
		List<String> prim = new LinkedList<>(primaryKeys());
		prim.remove(columnName);
		return withImmutableMessage(message().without(columnName)).withPrimaryKeys(Collections.unmodifiableList(prim));
	}
	
	@Override
	public ReplicationMessage without(List<String> columns) {
		List<String> prim = new LinkedList<>(primaryKeys());
		prim.removeAll(columns);
		return withImmutableMessage(message().without(columns)).withPrimaryKeys(Collections.unmodifiableList(prim));
	}

	@Override
	public ReplicationMessage rename(String columnName, String newName) {
		int keyIndex = this.primaryKeys.indexOf(columnName);
		List<String> primary =  this.primaryKeys;
		if(keyIndex!=-1) {
			primary = new ArrayList<>(this.primaryKeys);
			primary.set(keyIndex, newName);
		}
		return withImmutableMessage(message().rename(columnName,newName)).withPrimaryKeys(primary);
	}

	@Override
	public ReplicationMessage with(String key, Object value, ValueType type) {
		return withImmutableMessage(message().with(key, value, type));
	}
	
	@Override
	public ReplicationMessage withPrimaryKeys(List<String> primary) {
		return new ReplicationImmutableMessageImpl(this.source, this.partition,this.offset, this.transactionId,this.operation,this.timestamp, message(),primary,this.commitAction,this.paramMessage);
	}

	@Override
	public String toFlatString(ReplicationMessageParser parser) {
		return parser.describe(this);
	}

	@Override
	public ReplicationMessage withoutSubMessages(String field) {
		return withImmutableMessage(message().withoutSubMessages(field));
	}


	
	@Override
	public ReplicationMessage withoutSubMessage(String field) {
		return withImmutableMessage(message().withoutSubMessage(field));
	}

	@Override
	public Map<String, ImmutableMessage> subMessageMap() {
		return message().subMessageMap();
	}

	@Override
	public Map<String, List<ImmutableMessage>> subMessageListMap() {
		return message().subMessageListMap();
	}

	@Override
	public ReplicationMessage merge(ReplicationMessage other, Optional<List<String>> only) {
		return withImmutableMessage(message().merge(other.message(), only));
	}


	@Override
	public ReplicationMessage withOnlySubMessages(List<String> subMessages) {
		return withImmutableMessage(message().withOnlySubMessages(subMessages));
	}
	
	@Override
	public ReplicationMessage withOnlyColumns(List<String> columns) {
		return withImmutableMessage(message().withOnlyColumns(columns));
	}

	public ReplicationMessage withAllSubMessageLists(Map<String, List<ImmutableMessage>> subMessageListMap) {
		return withImmutableMessage(message().withAllSubMessageLists(subMessageListMap));
	}
	public ReplicationMessage withAllSubMessage(Map<String, ImmutableMessage> subMessageMap) {
		return withImmutableMessage(message().withAllSubMessage(subMessageMap));
	}

	@Override
	public ReplicationMessage withAddedSubMessage(String field, ImmutableMessage message) {
		return withImmutableMessage(message().withAddedSubMessage(field,message));
	}

	@Override
	public ReplicationMessage withoutSubMessageInList(String field,Predicate<ImmutableMessage> selector) {
		return withImmutableMessage(message().withoutSubMessageInList(field,selector));
	}
	@Override
	public ReplicationMessage withOperation(Operation operation) {
		return new ReplicationImmutableMessageImpl(this.source, this.partition,this.offset,this.transactionId,operation, this.timestamp, message(),primaryKeys,this.commitAction,this.paramMessage);
	}

	@Override
	public Map<String, Object> flatValueMap(boolean ignoreNull, Set<String> ignore, String prefix) {
		Map<String, Object> param = paramMessage.map(prm->prm.flatValueMap(ignoreNull, ignore, prefix+"@param")).orElse(Collections.emptyMap());
		Map<String, Object> flatValueMap = message().flatValueMap(ignoreNull, ignore, prefix);
		Map<String,Object> combined = new HashMap<String, Object>(flatValueMap);
		combined.putAll(param);
		return Collections.unmodifiableMap(combined);
	}

	public Map<String, Object> flatValueMap(String prefix, Trifunction processType) {
		return message().flatValueMap(prefix, processType);
	}
	
	
	public boolean equalsToMessage(ReplicationMessage c) {
		Map<String,Object> other = c.flatValueMap(false,Collections.emptySet(), "");
		final Map<String, Object> myMap = this.flatValueMap(false, Collections.emptySet(), "");
		return myMap.equals(other);
	}

	@Override
	public ReplicationMessage now() {
		return atTime(new Date().getTime());
	}

	@Override
	public ReplicationMessage atTime(long timestamp) {
		return new ReplicationImmutableMessageImpl(this.source, this.partition,this.offset,this.transactionId,this.operation, timestamp,this.message(), this.primaryKeys,commitAction,this.paramMessage);
	}

	@Override
	public Map<String, Object> values() {
		return message().values();
	}
	
	public ReplicationMessage withCommitAction(Runnable commitAction) {
		return new ReplicationImmutableMessageImpl(this.source,this.partition,this.offset, this.transactionId, this.operation, timestamp, message() , this.primaryKeys,Optional.of(commitAction),this.paramMessage);
	}


	@Override
	public Optional<String> source() {
		return this.source;
	}


	@Override
	public ReplicationMessage withSource(Optional<String> source) {
		return new ReplicationImmutableMessageImpl(source,this.partition,this.offset, this.transactionId, this.operation, timestamp, message() , this.primaryKeys,this.commitAction,this.paramMessage);
	}


	@Override
	public Optional<Integer> partition() {
		return this.partition;
	}


	@Override
	public Optional<Long> offset() {
		return this.offset;
	}


	@Override
	public ReplicationMessage withPartition(Optional<Integer> partition) {
		
		return new ReplicationImmutableMessageImpl(source, partition, this.offset, this.transactionId, this.operation, timestamp, message() , this.primaryKeys,this.commitAction,this.paramMessage);
//		public ReplicationImmutableMessageImpl(Optional<String> source, Optional<Integer> partition, Optional<Long> offset, String transactionId, Operation operation, long timestamp, Map<String,Object> values, Map<String,String> types,Map<String,ImmutableMessage> submessage, Map<String,List<ImmutableMessage>> submessages, List<String> primaryKeys, Optional<Runnable> commitAction) {

	}


	@Override
	public ReplicationMessage withOffset(Optional<Long> offset) {
		return new ReplicationImmutableMessageImpl(source,this.partition,offset, this.transactionId, this.operation, timestamp, message() , this.primaryKeys,this.commitAction,this.paramMessage);
	}

	public Optional<ImmutableMessage> paramMessage() {
		return this.paramMessage;
	}

	@Override
	public ReplicationMessage withParamMessage(ImmutableMessage msg) {
		return new ReplicationImmutableMessageImpl(this.source,this.partition,offset, this.transactionId, this.operation, timestamp, message() , this.primaryKeys,this.commitAction,Optional.of(msg));
	}

	@Override
	public ReplicationMessage withoutParamMessage() {
		return new ReplicationImmutableMessageImpl(this.source,this.partition,offset, this.transactionId, this.operation, timestamp, message() , this.primaryKeys,this.commitAction,Optional.empty());
	}




}

