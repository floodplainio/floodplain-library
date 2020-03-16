package com.dexels.immutable.json;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class ImmutableJSON {
	
	private static final Logger logger = LoggerFactory.getLogger(ImmutableJSON.class);
	public static final ObjectMapper objectMapper = new ObjectMapper();

	private ImmutableJSON() {
		// -- no instances
	}
	public static ObjectNode flatJson(ImmutableMessage msg) throws IOException {
		ObjectNode node = objectMapper.createObjectNode();
		msg.toTypedDataMap().entrySet().forEach(e->resolveValue(objectMapper,node,e.getKey(), e.getValue().type.name().toLowerCase(),Optional.ofNullable(e.getValue().value),false));
		for (Entry<String,ImmutableMessage> e : msg.subMessageMap().entrySet()) {
			flatJson(e.getValue(), node, e.getKey());
		}
		return node;
	}

	
	public static ObjectNode flatJson(ImmutableMessage msg, ObjectNode node, String prefix) throws IOException {
		msg.toTypedDataMap().entrySet().forEach(e->resolveValue(objectMapper,node,prefix+"_"+e.getKey(), e.getValue().type.name().toLowerCase(),Optional.ofNullable(e.getValue().value),false));
		for (Entry<String,ImmutableMessage> e : msg.subMessageMap().entrySet()) {
			flatJson(e.getValue(), node, prefix+"_"+e.getKey());
		}
		return node;
	}
	
	public static ObjectNode json(ImmutableMessage msg) throws IOException {
		ObjectNode node = objectMapper.createObjectNode();
		msg.toTypedDataMap().entrySet().forEach(e->resolveValue(objectMapper,node,e.getKey(), e.getValue().type.name().toLowerCase(),Optional.ofNullable(e.getValue().value),false));
		for (Entry<String,List<ImmutableMessage>> e : msg.subMessageListMap().entrySet()) {

			String key = e.getKey();
			ArrayNode nd = objectMapper.createArrayNode();
			for (ImmutableMessage submsg : e.getValue()) {
				nd.add(json(submsg));
			}
			node.set(key, nd);
		}
		for (Entry<String,ImmutableMessage> e : msg.subMessageMap().entrySet()) {
			node.set(e.getKey(), json(e.getValue()));
		}
		return node;
	}
	
	public static String ndJson(ImmutableMessage msg) throws IOException {
		return objectMapper.writeValueAsString(json(msg));
	}
	
	
	public static byte[] jsonSerializer(ImmutableMessage msg, boolean includeNullValues,boolean includeTypes) {
		try {
			ObjectWriter w = objectMapper.writerWithDefaultPrettyPrinter();
			return w.writeValueAsBytes(ImmutableJSON.toJSON(msg,includeNullValues,includeTypes));
		} catch (JsonProcessingException e) {
			logger.error("JSON parsing failing with value: {} types: {} submessagenames: {}",msg.values(),msg.types(),msg.subMessageMap().keySet());
			logger.error("Error serializing, got json error:", e);
			if (e.getCause() != null) {
		         logger.error("Error serializing:", e);
			}
			try {
		         logger.error("JSON failed to write: {}",ImmutableJSON.toJSON(msg,includeNullValues,includeTypes));
			} catch (Throwable t) {
			    //
			}

			return ("{}").getBytes();
		}		
	}

	public static ObjectNode toJSON(ImmutableMessage msg, boolean includeNullValues, boolean includeTypes) {
		ObjectNode result = objectMapper.createObjectNode();
		msg.columnNames().forEach(column->{
			String type = msg.columnType(column);
			result.put(column+".type", type);
			resolveValue(objectMapper, result,column,type,msg.value(column),includeNullValues);
		});
		msg.subMessageListMap().entrySet().forEach(e->{
			String key = e.getKey();
			ArrayNode nd = objectMapper.createArrayNode();
			e.getValue().stream().map(x->toJSON(x, includeNullValues,includeTypes)).forEach(nd::add);
			result.set(key, nd);
		});
		msg.subMessageMap().entrySet().forEach(e->{
			String key = e.getKey();
			result.set(key, toJSON(e.getValue(), includeNullValues,includeTypes));
		});
		return result;
	}


	private static void resolveValue(ObjectMapper objectMapper, ObjectNode m, String key, String type, Optional<Object> maybeValue,boolean includeNullValues) {
		if(!maybeValue.isPresent() ) {
			if(includeNullValues) {
				m.putNull(key);
			}
			return;
		}
		if(type==null) {
			logger.error("Null type for key: {}",key);
			return;
		}
		Object value = maybeValue.get();
		switch (type) {
			case "string":
			case "binary_digest":
				m.put(key, (String)value);
				return;
			case "integer":
				m.put(key, (Integer)value);
				return;
			case "long":
				m.put(key, (Long)value);
				return;
			case "double":
			case "float":
				m.put(key, (Double)value);
				return;
			case "boolean":
				m.put(key, (Boolean)value);
				return;
			case "date":
				if(value instanceof String) {
					m.put(key, (String)value);
				} else {
					String t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS").format((Date)value);
					m.put(key, t);
				}
				return;
			case "clocktime":
				if(value instanceof String) {
					m.put(key, (String)value);
				} else {
				    String c = new SimpleDateFormat("HH:mm:ss").format((Date)value);
					m.put(key, c);
				}
				
                return;
			case "list":
				if(value instanceof List) {
		            ArrayNode arrayNode = m.putArray(key);
		            @SuppressWarnings("rawtypes") 
		            ArrayNode valueToTree = objectMapper.valueToTree((List)value);
		            arrayNode.addAll(valueToTree);
				} else {
					logger.warn("Bad type mapping, key: {} of type: list has actual class: {}. Treating as string for now.",key,value.getClass());
					String stringify = value.toString();
					m.put(key, stringify);
				}
	            break;
			default:
				break;
	}

	}
	

	
	private static Object resolveValue(String type, JsonNode jsonNode) {
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
			default:
			    logger.warn("Unsupported type {}", type);
				break;
		}
		return null;
	}

	public static ImmutableMessage parseJSON(ObjectNode node) {
		Map<String,JsonNode> localvalues = new HashMap<>();
		Map<String,String> localtypes = new HashMap<>();
		Map<String,ImmutableMessage> subMessageMap = new HashMap<>();
		Map<String,List<ImmutableMessage>> subMessageListMap = new HashMap<>();
		
		final Iterator<Entry<String, JsonNode>> fields = node.fields();
		while (fields.hasNext()) {
			Entry<String,JsonNode> e = fields.next();
			final JsonNode value = e.getValue();
			if(value.isObject()) {
				ObjectNode on = (ObjectNode) value;
				ImmutableMessage submsg = parseJSON(on);
				subMessageMap.put(e.getKey(), submsg);
			} else if(value.isArray()) {
				ArrayNode an = (ArrayNode) value;
				List<ImmutableMessage> submsg = new ArrayList<>();
				for (JsonNode jsonNode : an) {
					ImmutableMessage sm = parseJSON((ObjectNode) jsonNode);
					submsg.add(sm);
				}
				subMessageListMap.put(e.getKey(), submsg);
			} else {
				String name = e.getKey();
				if(name.endsWith(".type")) {
					String type = name.substring(0, name.length() - ".type".length());
					localtypes.put(type, e.getValue().asText());
				} else {
					localvalues.put(e.getKey(), e.getValue());
				}
			}
		}
		Map<String,Object> values = new HashMap<>();

		localvalues.entrySet()
			.stream()
			.forEach(e->{
				Object value = resolveValue(localtypes.get(e.getKey()),e.getValue());
				values.put(e.getKey(), value);
			});
		return ImmutableFactory.create(values, localtypes);
	}



}


