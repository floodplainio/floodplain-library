package com.dexels.kafka.converter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ReplicationMessageConverter implements Converter {


	private boolean isKey = false;
	
	private final static Logger logger = LoggerFactory.getLogger(ReplicationMessageConverter.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();
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
				logger.info("Converting key: {} into {}",val,converted);
				return converted.getBytes(Charset.defaultCharset());
			} else if (value instanceof Struct) {
				System.err.println("Schema present? "+schema);
//				schema.
				Struct val = (Struct)value;
				System.err.println("struct: "+val);
//				Schema
//				val.
//				
			}
		}
		ReplicationMessage rm = (ReplicationMessage) value;
		return ReplicationFactory.getInstance().serialize(rm);
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		logger.info("toConnectData topic: {}, value: {}",topic,value.length);
		if(isKey) {
			try {
				JsonNode jn = objectMapper.readTree(value);
				logger.info("toConnect type: {}",jn.getClass().getName());
				if(jn instanceof ObjectNode) {
					return new SchemaAndValue(null, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jn));
//					ObjectNode on = (ObjectNode) objectMapper.readTree(value);
//					logger.info("toConnect objectnode. {}",on.get("key").asText());
//					return new SchemaAndValue(null, on.get("key").asText());
				} else {
					ObjectNode on = objectMapper.createObjectNode();
					on.put("key", jn.asText());
//					;
					return new SchemaAndValue(null,objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(on));
				}

			} catch (IOException e) {
				if(value!=null) {
					logger.error("Error writing value: "+new String(value));
				}
				throw new RuntimeException("Error parsing key bytes: ",e);
			}
		} else {
			ReplicationMessage replMessage = ReplicationFactory.getInstance().parseBytes(Optional.empty(),value);
//			replMessage.valueMap(true, Collections.emptySet());
			return new SchemaAndValue(null, replMessage.valueMap(true, Collections.emptySet()));
		}
	}

}
