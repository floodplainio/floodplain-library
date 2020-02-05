package com.dexels.kafka.streams.debezium;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.StreamOperators;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.factory.PubSubTools;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.factory.ReplicationFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JSONToReplicationMessage {
	
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	
	private final static Logger logger = LoggerFactory.getLogger(JSONToReplicationMessage.class);

	
	//TODO Beware of threading issues
	private final static DateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SS");

	
	public static boolean isValid(ObjectNode node) {
		return !node.get("schema").isNull();
	}

	public static PubSubMessage parse(TopologyContext context, PubSubMessage msg, boolean appendTenant, boolean appendSchema, boolean appendTable) {
		try {
			ObjectNode keynode = (ObjectNode) objectMapper.readTree(msg.key());
			ObjectNode valuenode = (ObjectNode) objectMapper.readTree(msg.value());
			TableIdentifier key = processDebeziumKey(keynode,appendTenant,appendSchema);
			
			if(!valuenode.has("payload") || valuenode.get("payload").isNull()) {
				ReplicationMessage replMsg = ReplicationFactory.empty().with("_table", key.table, "string").withOperation(Operation.DELETE);
				final ReplicationMessage converted = appendTenant ? replMsg.with("_tenant", key.tenant, "string") : replMsg;
				return PubSubTools.create(key.combinedKey,  ReplicationFactory.getInstance().serialize(converted), msg.timestamp(), Optional.empty());
			}
			final ReplicationMessage convOptional = convertToReplication(false,valuenode,key.table);
			ReplicationMessage conv = convOptional;
			conv = conv.with("_table", key.table, "string")
				.withPrimaryKeys(key.fields);
			final ReplicationMessage converted = appendTenant ? conv.with("_tenant", key.tenant, "string") : conv;
			byte[] serialized = ReplicationFactory.getInstance().serialize(converted);
			
//			logger.info("Forwarding to: {}",context.topicName(key.table));
			return PubSubTools.create(key.combinedKey, serialized, msg.timestamp(), Optional.of(CoreOperators.topicName(key.table, context)),msg.partition(),msg.offset());
		} catch (JsonProcessingException e) {
			logger.error("Error: ", e);
		} catch (IOException e) {
			logger.error("Error: ", e);
		}
		return null;
	}
	


	public static ImmutableMessage convert(ObjectNode node, Consumer<String> callbackFieldList,boolean isKey, Optional<Operation> o, String table) {
		if(!isKey && o.isPresent() && o.get().equals(Operation.DELETE)) {
			System.err.println("DELETE DETECTED!");
			return ImmutableFactory.empty().with("table", table, "string");
		}
		try {
			JsonNode payLoad = node.get("payload");
			final Optional<ObjectNode> payload = payLoad.isNull() ? Optional.empty() : Optional.of((ObjectNode)payLoad);
			final JsonNode schema =  node.get("schema");
			if(schema.isNull()) {
				System.err.println("WRITING FAILED: "+objectMapper.writeValueAsString(node));
			}
			ArrayNode fields = (ArrayNode) schema.get("fields");
			if(!isKey) {
				Optional<JsonNode> firstFields = findFirstChild(fields,e->true);
				fields = firstFields.isPresent() ? (ArrayNode)firstFields.get().get("fields") : fields;
			}

			Map<String,String> types = new HashMap<>();
			Map<String,String> typeNames = new HashMap<>();
			Map<String,Object> jsonValues = new HashMap<>();

			fields.forEach(e->{
				String field = e.get("field").asText();
				callbackFieldList.accept(field);
				JsonNode name = e.get("name");
				Optional<String> typeName = name==null?Optional.empty():Optional.of(name.asText());
				if(typeName.isPresent()) {
					typeNames.put(field, typeName.get());
				}
				String rawType = e.get("type").asText();
				String type = resolveType(rawType,typeName);
				types.put(field, type);
				boolean hasAfter = payload.get().has("after");
				boolean reallyHasAfter = hasAfter && !payload.get().get("after").isNull();
				final Optional<ObjectNode> after = reallyHasAfter ? Optional.ofNullable((ObjectNode)payload.get().get("after")) : Optional.empty();
				final Object resolvedValue = reallyHasAfter ? resolveValue(after, field,type, typeName) : resolveValue(payload, field,type,typeName);
				jsonValues.put(field,resolvedValue);
			});
			return ImmutableFactory.create(jsonValues, types);

		} catch (JsonProcessingException e1) {
			logger.error("Error: ", e1);
		}
		return ImmutableFactory.empty();
	}
	
	public static ReplicationMessage convertToReplication(boolean isKey, ObjectNode node, String table) {
		List<String> fields = new ArrayList<>();
		ObjectNode payload = (ObjectNode) node.get("payload");
		long millis = payload.get("ts_ms").asLong();
		Operation o = resolveOperation(payload,payload.get("op").asText());
		ImmutableMessage core = convert(node,field->fields.add(field),isKey,Optional.of(o),table);
		return ReplicationFactory.standardMessage(core).withOperation(o).atTime(millis);
	}
	
	
	private static Optional<JsonNode> findFirstChild(ArrayNode node, Predicate<JsonNode> pred) {
		return StreamSupport.stream(node.spliterator(),false).filter(pred).findFirst();
	}
	
	private static Object resolveValue(Optional<ObjectNode> fields, String field, String type, Optional<String> typeName) {
		try {
			JsonNode node = fields.get().get(field);
			if(node==null) {
				throw new NullPointerException("Missing node for field: "+field+" type: "+type+" typeName: "+typeName);
			}
			return resolveValue(type, typeName,node);
		} catch (Throwable e) {
			throw new RuntimeException("Error resolving value: "+field+" with type: "+type+" named type: "+typeName,e);
		}
	}
	
	public static String resolveType(String type, Optional<String> namedType) {
		if(!namedType.isPresent()) {
			return resolveSimpleType(type);
		}
		switch (namedType.get()) {
			case "io.debezium.time.Date":
				return "date";
			case "io.debezium.time.ZonedTimestamp":
				return "long";
			case "io.debezium.time.NanoTimestamp":
				return "long";
			case "io.debezium.time.MicroTimestamp":
				return "long";
			case "io.debezium.data.VariableScaleDecimal":
				return "long";
			case "org.apache.kafka.connect.data.Decimal":
				return "long";
			case "io.debezium.data.Enum":
				return "enum";
			default:
				logger.warn("Unknown type with name, this will probably fail: {}",namedType.get());
				return resolveSimpleType(type);
		}
		
	}

	public static Object resolveValue(String type, Optional<String> namedType, JsonNode value) {
		
		if(value.isNull()) {
			return null;
		}
		if(!namedType.isPresent()) {
			return resolveSimple(type,value);
		}
		switch (namedType.get()) {
		case "io.debezium.time.Date":
 			int valueInt = value.asInt();
 			// I have no clue...
 			long timemillis = 24*60*60*1000*(long)valueInt;
 			Calendar c = Calendar.getInstance();
 			c.add(Calendar.DAY_OF_YEAR, valueInt);
 			Date d = new Date(timemillis);
			return format.format(d);
		case "io.debezium.time.ZonedTimestamp":
			Instant instant = Instant.parse (value.asText() ) ;
			return instant.toEpochMilli();
		case "io.debezium.time.NanoTimestamp":
			long l2 = Long.parseLong(value.asText());
			Instant instant2 = Instant.ofEpochMilli(l2);
			return instant2.toEpochMilli();
		case "io.debezium.data.VariableScaleDecimal":
			ObjectNode node = (ObjectNode) value;
			int scale = node.get("scale").asInt();
			String val = node.get("value").asText();
			byte[] binary = Base64.getDecoder().decode(val);
			final BigDecimal decoded = new BigDecimal(new BigInteger(binary), scale);
			logger.info("VariableScale: {} -> decoded length: {}",decoded,binary.length);
			return decoded.longValue();
		case "org.apache.kafka.connect.data.Decimal":
			String decval = value.asText();
			byte[] da = Base64.getDecoder().decode(decval);
			
			return bytesToLong(da);
		case "io.debezium.data.Enum":
			return value.asText();
		default:
			return resolveSimple(type,value);
		}
	}
	
	private static long bytesToLong(byte[] bytes) {
//		logger.info("long byte size: {}",bytes.length);
		switch(bytes.length) {
		case 1:
		    ByteBuffer bytebuffer = ByteBuffer.allocate(Long.BYTES);
		    bytebuffer.put(bytes);
		    bytebuffer.flip();//need flip 
		    return bytebuffer.get();
		case 2:
		    ByteBuffer shortbuffer = ByteBuffer.allocate(Long.BYTES);
		    shortbuffer.put(bytes);
		    shortbuffer.flip();//need flip 
		    return shortbuffer.getShort();
		case 4:
		    ByteBuffer intbuffer = ByteBuffer.allocate(Long.BYTES);
		    intbuffer.put(bytes);
		    intbuffer.flip();//need flip 
		    return intbuffer.getInt();
		default:
		    ByteBuffer longbuffer = ByteBuffer.allocate(Long.BYTES);
		    longbuffer.put(bytes);
		    longbuffer.flip();//need flip 
		    return longbuffer.getInt();
		}

	}
	
private static Object resolveSimple(String type, JsonNode value) {
	switch (type) {
		case "int16":
		case "integer":
		case "int32":
			return value.asInt();
		case "int64":
		case "long":
			return value.asLong();
		case "string":
			return value.asText();
		case "boolean":
			return value.asBoolean();
		case "double":
			return value.asDouble();
		case "binary":
			return Base64.getDecoder().decode(value.asText());
		case "bytes":
			return Base64.getDecoder().decode(value.asText());
		case "list":
			List<String> ar = new ArrayList<>();
			((ArrayNode)value).forEach(e->ar.add(e.asText()));
			return Collections.unmodifiableList(ar);
		default:
			throw new RuntimeException("Unknown type: "+type);
		}
	}

	private static String resolveSimpleType(String type) {
		switch (type) {
		case "int16":
		case "int32":
			return "integer";
		case "int64":
			return "long";
		case "string":
			return "string";
		case "double":
			return "double";
		case "bytes":
			return "binary";
		case "array":
			return "list";
		case "boolean":
			return "boolean";
		default:
			throw new RuntimeException("Unknown type: "+type);
		}
	}
// TODO Complete when I have more examples;
	private static Operation resolveOperation(ObjectNode payloadNode, String opName) {
		boolean hasBefore = payloadNode.has("before");
		boolean hasAfter = payloadNode.has("after");
		if(hasBefore && !hasAfter) {
			if(!"d".equals(opName)) {
				logger.warn("Unexpected operation: "+opName);
			}
			return Operation.DELETE;
		}
		switch(opName) {
			case "u":
			case "r":
				return Operation.UPDATE;
			case "c":
				return Operation.INSERT;
			case "d":
				return Operation.DELETE;
			default:
				return Operation.NONE;
		}
	}
	


	public static String mapType(String dbsType, Optional<String> typeName) {
		final String resolvedType = resolveType(dbsType, typeName);
		return resolvedType;
	}
	
	public static TableIdentifier processDebeziumKey(ObjectNode on, boolean appendTenant, boolean appendSchema) {
		List<String> fields = new ArrayList<>();
		ImmutableMessage converted = convert(on,field->fields.add(field),true,Optional.empty(),null);
		String tableId = (String) converted.columnValue("__dbz__physicalTableIdentifier");
		fields.remove("__dbz__physicalTableIdentifier");
		// for demo, shouldn't do any harm
		if(tableId==null) {
			tableId = on.get("schema").get("name").asText();
		}
//		String key = fields.stream()
//			.map(field->converted.columnValue(field).toString())
//			.collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
		
		// prepend:
		TableIdentifier tid = new TableIdentifier(tableId,converted,fields,appendTenant,appendSchema);
//		key = tid.combinedKey;
		return tid;
	}
}
