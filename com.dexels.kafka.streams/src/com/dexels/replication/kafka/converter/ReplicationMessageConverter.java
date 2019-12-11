package com.dexels.replication.kafka.converter;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;

public class ReplicationMessageConverter implements Converter {

	private final FallbackReplicationMessageParser parser = new FallbackReplicationMessageParser();

	
	private final static Logger logger = LoggerFactory.getLogger(ReplicationMessageConverter.class);

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		return null;
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
//		String datastring = new String(value);
		
        ReplicationMessage message;
		try {
			message = parser.parseBytes(value);
	        return new SchemaAndValue(null, message);
		} catch (Throwable e) {
			logger.error("Error: ", e);
			return new SchemaAndValue(null, null);
		}
	}

}
