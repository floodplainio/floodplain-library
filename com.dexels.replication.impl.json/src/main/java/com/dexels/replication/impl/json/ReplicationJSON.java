package com.dexels.replication.impl.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.customtypes.CoordinateType;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.factory.ReplicationFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ReplicationJSON {
	
	private final static Logger logger = LoggerFactory.getLogger(ReplicationJSON.class);
	public static final ObjectMapper objectMapper = new ObjectMapper();


	public static ReplicationMessage parseReplicationMessage(byte[] data,Optional<String> source, ObjectMapper objectMapper) throws JsonProcessingException, IOException {
		 return ReplicationJSON.parseJSON(source, (ObjectNode)parseJSON(data,objectMapper));
	}
	
	public static ReplicationMessage parseReplicationMessage(InputStream stream,Optional<String> source,ObjectMapper objectMapper) throws JsonProcessingException, IOException {
		 return ReplicationJSON.parseJSON(source, (ObjectNode)parseJSON(stream,objectMapper));
	}

	public static byte[] jsonSerializer(ReplicationMessage msg, boolean includeNullValues) {
		return jsonSerializer(msg, includeNullValues, false);
	}
	public static byte[] jsonSerializer(ReplicationMessage msg, boolean includeNullValues, boolean dumpKey) {
		try {
			ObjectWriter w = ReplicationMessage.usePrettyPrint()?objectMapper.writerWithDefaultPrettyPrinter():objectMapper.writer();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			if(dumpKey) {
				baos.write(msg.combinedKey().getBytes());
				baos.write('\t');
			}
			w.writeValue(baos,ReplicationJSON.toJSON(msg,includeNullValues));
			baos.write('\n');
			return baos.toByteArray();
		} catch (JsonProcessingException e) {
			logger.error("JSON parsing failing with key: {} value: {} types: {} submessagenames: {}",msg.queueKey(),msg.values(),msg.types(),msg.subMessageMap().keySet());
			logger.error("Error serializing, got json error:", e);
			if (e.getCause() != null) {
		         logger.error("Error serializing cause:", e.getCause());
		         e.printStackTrace();
			}
			try {
		         logger.error("JSON failed to write: {}",ReplicationJSON.toJSON(msg,includeNullValues).toString());
			} catch (Throwable t) {
			    
			}

			return (new String("{}")).getBytes();
		} catch (IOException e) {
			throw new RuntimeException("Weird json problem",e);
		}		
	}

	public static String jsonDescriber(ReplicationMessage msg, ReplicationMessageParser parser) {
		return msg.flatValueMap(true, Collections.emptySet(), "").toString();
	}
	
	private static JsonNode parseJSON(byte[] data,ObjectMapper objectMapper) throws JsonProcessingException, IOException {
	    try {
	        JsonNode res =  objectMapper.readTree(data);
	        return res;
	    } catch (Throwable t) {
	        logger.error("Exception on parsing json! {}", new String(data), t);
	        throw t;
	    }
	    
	}

	private static JsonNode parseJSON(InputStream stream,ObjectMapper objectMapper) throws JsonProcessingException, IOException {
		return objectMapper.readTree(stream);
	}
	
	static void resolveValue(ObjectMapper objectMapper, ObjectNode m, String key, String type, Object value,boolean includeNullValues) {
		if(value==null ) {
			if(includeNullValues) {
				m.putNull(key);
			}
			return;
		}
		if(type==null) {
			logger.info("Null type for key: "+key);
		}
		switch (type) {
			case "string":
				m.put("Value", (String)value);
				return;
			case "integer":
				m.put("Value", (Integer)value);
				return;
			case "long":
				m.put("Value", (Long)value);
				return;
			case "double":
				m.put("Value", (Double)value);
				return;
			case "float":
				if (value instanceof Float) {
					m.put("Value", (Float)value);
				} else {
					m.put("Value", (Double)value);
				}
				return;
			case "boolean":
				m.put("Value", (Boolean)value);
				return;
			case "binary_digest":
				m.put("Value", (String)value);
				return;
			case "date":
				if(value instanceof String) {
					m.put("Value", (String)value);
				} else {
					String t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS").format((Date)value);
					m.put("Value", t);
				}
				return;
			case "clocktime":
				if(value instanceof String) {
					m.put("Value", (String)value);
				} else {
				    String c = new SimpleDateFormat("HH:mm:ss").format((Date)value);
					m.put("Value", c);
				}
				
                return;
			case "list":
	            ArrayNode arrayNode = m.putArray("Value");
	            @SuppressWarnings("rawtypes") 
	            ArrayNode valueToTree = objectMapper.valueToTree((List)value);
	            arrayNode.addAll(valueToTree);
	            break;
			case "binary":
				m.put("Value",Base64.getEncoder().encodeToString((byte[])value));
				break;
			case "coordinate":
			    m.put("Value", ((CoordinateType)value).toString());
			    break;
			case "enum":
			    m.put("Value", (String)value);
			    break;
			default:
				logger.warn("Unknown type: {} while serializing replication message to JSON. ",type);
				break;
		}
	}
	

	
	public static Object resolveValue(String type, JsonNode jsonNode) {
		if(jsonNode == null) {
			return null;
		}
		if(jsonNode instanceof NullNode) {
			return null;
		}
		
		switch (type) {
			case "string":
				return jsonNode.asText();
			case "integer":
				return jsonNode.asInt();
			case "long":
				return jsonNode.asLong();
			case "double":
				return jsonNode.asDouble();
			case "float":
				return jsonNode.asDouble();
			case "boolean":
				return jsonNode.asBoolean();
			case "binary_digest":
				return jsonNode.asText();
			case "binary":
				return jsonNode.isNull() ? new byte[]{} : Base64.getDecoder().decode(jsonNode.asText());
			case "list":
			    ArrayNode node = ((ArrayNode) jsonNode);
			    List<Object> result = new ArrayList<>();
			    for (final JsonNode objNode : node) {
			        if (objNode.isInt()) {
			            result.add(objNode.asInt());
			        } else if (objNode.isTextual()) {
			            result.add(objNode.asText());
			        } else {
			            logger.warn("Unsupported array element type: {} in {}. Ignoring!", objNode,jsonNode );
			        }
			    }
			    return result;
			case "date":
				//"2011-10-03 15:01:06.00"
				try {
					return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS").parse(jsonNode.asText());
				} catch (ParseException e) {
				    logger.warn("Cannot parse date {} = returning null", jsonNode.asText());
					return null;
				}
			case "clocktime":
                //"15:01:06"
                try {
                    return new SimpleDateFormat("HH:mm:ss").parse(jsonNode.asText());
                } catch (ParseException e) {
                    logger.warn("Cannot parse clocktime {} = returning null", jsonNode.asText());

                    return null;
                }
        case "coordinate":
            try {
                return new CoordinateType(jsonNode.asText());
            } catch (Exception e) {
                logger.warn("Cannot parse coordinate {} = returning null", jsonNode.asText());

                return null;
            }
        case "enum":
        	return jsonNode.asText();
        default:
			    logger.warn("Unsupported type {}", type);
				break;
		}
		return null;
	}

	public static ObjectNode toJSON(ReplicationMessage instance, boolean includeNullValues) {
		ObjectNode node = objectMapper.createObjectNode();
		try {
			if(instance.transactionId()!=null) {
				node.put("TransactionId", instance.transactionId());
			}
			node.put("Timestamp", instance.timestamp());
			node.put("Operation", instance.operation().toString());
			ArrayNode keys = objectMapper.createArrayNode();
			for (String key : instance.primaryKeys()) {
				keys.add(key);
			}
			node.set("PrimaryKeys",keys);
			ImmutableMessage immutable = instance.message();
			appendImmutable(node, immutable, includeNullValues);
		} catch (Throwable e) {
			logger.error("Error serializing replicationmessage "+instance.queueKey()+" columns: "+instance.values(),e);
		}
		return node;
		
	}

	private static void appendImmutable(ObjectNode node, ImmutableMessage immutable, boolean includeNullValues) {
		ObjectNode columns = objectMapper.createObjectNode();
		node.set("Columns",columns);
		final Map<String, String> types = immutable.types();
		for (Entry<String,Object> e : immutable.values().entrySet()) {
			ObjectNode m = objectMapper.createObjectNode();
			final String key = e.getKey();
			final String type = types.get(key);
			m.put("Type", type);
			ReplicationJSON.resolveValue(objectMapper, m,key,type,e.getValue(),includeNullValues);
			columns.set(e.getKey(), m);
		}
		ObjectNode subMessage = null;
		if(immutable.subMessageMap()!=null || immutable.subMessageListMap()!=null) {
			subMessage = objectMapper.createObjectNode();
			node.set("SubMessage", subMessage);
		}
		if(subMessage!=null) {
			if(immutable.subMessageMap()!=null) {
				for (Entry<String, ImmutableMessage> entry : immutable.subMessageMap().entrySet()) {
					subMessage.set(entry.getKey(), toJSONImmutable(entry.getValue(),includeNullValues));
				}
			}
			if(immutable.subMessageListMap()!=null) {
				for (Entry<String, List<ImmutableMessage>> entry : immutable.subMessageListMap().entrySet()) {
					ArrayNode arraynode = objectMapper.createArrayNode();
					subMessage.set(entry.getKey(), arraynode);
					List<ImmutableMessage> list = entry.getValue();
					for (ImmutableMessage msg : list) {
						arraynode.add(toJSONImmutable(msg,includeNullValues));
					}
				}
			}
		}
	}

	private static ObjectNode toJSONImmutable(ImmutableMessage immutable, boolean includeNullValues) {
		ObjectNode node = objectMapper.createObjectNode();
		try {
			node.put("TransactionId", "");
			node.put("Timestamp", -1L);
			node.put("Operation", Operation.NONE.name());
			ArrayNode keys = objectMapper.createArrayNode();
			node.set("PrimaryKeys",keys);
			appendImmutable(node, immutable, includeNullValues);
		} catch (Throwable e) {
			logger.error("Error serializing replicationmessage.",e);
			e.printStackTrace();
		}
		return node;		
	}
	
	public static ReplicationMessage parseJSON(Optional<String> source, ObjectNode node) {
		
		final String transactionId = node.get("TransactionId")!=null ? node.get("TransactionId").asText() : null;
		final long timestamp = node.get("Timestamp")!=null ? node.get("Timestamp").asLong() : -1;
		final Operation operation = node.get("Operation")!=null ? Operation.valueOf(node.get("Operation").asText().toUpperCase()) : Operation.NONE;
		ArrayNode keys = (ArrayNode) node.get("PrimaryKeys");
		List<String> l;
		if(keys!=null) {
			l = new ArrayList<>();
			for (JsonNode key : keys) {
				l.add(key.asText());
			}
		} else {
			l = Collections.emptyList();
		}
		final List<String> primaryKeys = Collections.unmodifiableList(l);

		
		ImmutableMessage flatMessage = parseImmutable(node);
		ObjectNode paramMsg = (ObjectNode) node.get("ParamMessage");
		return ReplicationFactory.createReplicationMessage(source,Optional.<Integer>empty(),Optional.<Long>empty(), transactionId, timestamp, operation, primaryKeys,flatMessage, Optional.<Runnable>empty(),Optional.ofNullable(paramMsg).map(ReplicationJSON::parseImmutable));

	}

	private static ImmutableMessage parseImmutable(ObjectNode node) {
		ObjectNode val = (ObjectNode) node.get("Columns");
		Map<String,Object> localvalues = new HashMap<>();
		Map<String,String> localtypes = new HashMap<>();
		if(val!=null) {
			Iterator<Entry<String, JsonNode>> it = val.fields();
			while (it.hasNext()) {
				Entry<String, JsonNode> e = it.next();
				String name = e.getKey();
				ObjectNode column = (ObjectNode)e.getValue();
				JsonNode typeObject = column.get("Type");
				if(typeObject==null) {
					try {
						logger.error("Null type found in replication message: "+objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node));
					} catch (JsonProcessingException e1) {
						logger.error("Error: ", e1);
					}
				}
				String type = typeObject.asText();
				Object value = ReplicationJSON.resolveValue(type,column.get("Value"));
				localvalues.put(name, value);
				localtypes.put(name, type);
			}
		}
		Map<String,String> types = Collections.unmodifiableMap(localtypes);
		Map<String,Object> values = Collections.unmodifiableMap(localvalues);
		ObjectNode subMessage = (ObjectNode) node.get("SubMessage");

		Map<String,ImmutableMessage> subMessageMap;
		Map<String,List<ImmutableMessage>> subMessageListMap;
		
		if(subMessage!=null) {
			Map<String,ImmutableMessage> inProgressSubMessageMap = null;
			Map<String,List<ImmutableMessage>> inProgressSubMessageListMap = null;
			Iterator<Entry<String, JsonNode>> it = subMessage.fields();
			while (it.hasNext()) {
				Entry<String, JsonNode> e = it.next();
				String name = e.getKey();
				JsonNode json = e.getValue();
				if(json instanceof ObjectNode) {
					ObjectNode sub = (ObjectNode)json;
					ImmutableMessage subMessageImpl = parseImmutable(sub);
					if(inProgressSubMessageMap==null) {
						inProgressSubMessageMap = new HashMap<>();
					}
					inProgressSubMessageMap.put(name, subMessageImpl);
				} else if(json instanceof ArrayNode) {
					ArrayNode sub = (ArrayNode)json;
					List<ImmutableMessage> msgs = new LinkedList<>();
					for (JsonNode element : sub) {
						ImmutableMessage subMessageImpl = parseImmutable((ObjectNode)element);
						msgs.add(subMessageImpl);
					}
					if(inProgressSubMessageListMap==null) {
						inProgressSubMessageListMap = new HashMap<>();
					}
					inProgressSubMessageListMap.put(name, Collections.unmodifiableList(msgs));
				}
				
			}
			subMessageMap = inProgressSubMessageMap == null? Collections.emptyMap() : Collections.unmodifiableMap(inProgressSubMessageMap);
			subMessageListMap = inProgressSubMessageListMap == null? Collections.emptyMap() : Collections.unmodifiableMap(inProgressSubMessageListMap);
		} else {
			subMessageMap = Collections.emptyMap();
			subMessageListMap = Collections.emptyMap();
		}
		ImmutableMessage flatMessage = ImmutableFactory.create(localvalues, localtypes,subMessageMap,subMessageListMap);
		return flatMessage;
	}


//	public ReplicationMessageImpl(ReplicationMessageImpl parent, Operation operation, long timestamp, Map<String,Object> values, Map<String,String> types,Map<String,ReplicationMessage> submessage, Map<String,List<ReplicationMessage>> submessages, Optional<List<String>> primaryKeys) {

}


