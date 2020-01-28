package com.dexels.kafka.converter;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;

public class ReplicationMessageConverter implements Converter {


	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		ReplicationMessage rm = (ReplicationMessage) value;
		return ReplicationFactory.getInstance().serialize(rm);
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		return new SchemaAndValue(null, ReplicationFactory.getInstance().parseBytes(Optional.empty(),value));
	}

}
