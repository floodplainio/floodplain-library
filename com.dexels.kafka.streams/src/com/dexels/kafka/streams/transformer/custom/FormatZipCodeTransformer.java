package com.dexels.kafka.streams.transformer.custom;

import java.util.Map;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

public class FormatZipCodeTransformer implements MessageTransformer {

	@Override
	public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
		String field = params.get("field");
        Object valueObj = msg.columnValue(field);
        String value = (String) valueObj;
        if (value != null) {
            value = value.replace(" ", "").trim();
        }
        return msg.with(field, value, "string");
    }

}
