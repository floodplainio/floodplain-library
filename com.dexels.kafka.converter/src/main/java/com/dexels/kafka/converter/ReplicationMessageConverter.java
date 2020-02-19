package com.dexels.kafka.converter;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReplicationMessageConverter implements Converter {


	private boolean isKey = false;
	
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	private final static Logger logger = LoggerFactory.getLogger(ReplicationMessageConverter.class);
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		ReplicationFactory.setInstance(new FallbackReplicationMessageParser());
		logger.info("Initializer of ReplicationMessageConverter key: {}",isKey);
		logger.info("Configuration: {}",configs);
		this.isKey = isKey;
	}

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		logger.info("fromConnectData topic: {}, value: {}",topic,value);
		if(isKey) {
			if(value instanceof String) {
				String val = (String)value;
				String converted = "{key:\""+val+"\"}";
				return converted.getBytes(Charset.defaultCharset());
			} else if (value instanceof Struct) {
				Struct val = (Struct)value;
				String result = schema.fields().stream().map(e->""+val.get(e)).collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
				return result.getBytes(Charset.defaultCharset());
			} else {
				return (""+value).getBytes(Charset.defaultCharset());
			}
		}
		ReplicationMessage rm = (ReplicationMessage) value;
		return ReplicationFactory.getInstance().serialize(rm);
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		logger.info("toConnectData topic: {}, value: {}",topic,value==null?0: value.length);
		if(value==null) {
			return new SchemaAndValue(null, null);
		}
		if(isKey) {
			return toConnectDataKey(value);
		} else {
			ReplicationMessage replMessage = ReplicationFactory.getInstance().parseBytes(Optional.empty(),value);
			Map<String, Object> valueMap = replMessage.valueMap(true, Collections.emptySet());
//			objectMapper.writeValueAsString(valueMap);
			try {
				return new SchemaAndValue(null, objectMapper.writeValueAsString(valueMap));
			} catch (JsonProcessingException e) {
				throw new RuntimeException("Json issue",e);
//				e.printStackTrace();
			}
		}
	}

	private SchemaAndValue toConnectDataKey(byte[] value) {
		String valueString = new String(value);
		String converted = "{key:\""+valueString+"\"}";
		return new SchemaAndValue(null, converted);
//		try {
//			JsonNode jn = objectMapper.readTree(value);
//			logger.info("toConnect type: {}",jn.getClass().getName());
//			if(jn instanceof ObjectNode) {
//				return new SchemaAndValue(null, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jn));
//			} else {
//				ObjectNode on = objectMapper.createObjectNode();
//				on.put("key", jn.asText());
////					;
//				return new SchemaAndValue(null,objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(on));
//			}
//
//		} catch (IOException e) {
//			if(value!=null) {
//				logger.error("Error writing value: "+new String(value));
//			}
//			throw new RuntimeException("Error parsing key bytes: ",e);
//		}
	}

}
