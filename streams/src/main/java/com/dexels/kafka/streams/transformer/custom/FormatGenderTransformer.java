package com.dexels.kafka.streams.transformer.custom;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

import java.util.Map;

public class FormatGenderTransformer implements MessageTransformer {

	@Override
	public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
		String field = params.get("field");
		String outputfield = params.get("outputfield");
		if (field==null) {
			field = "Gender";
		}
		if (outputfield==null) {
			outputfield = "Gender";
		}
		if(msg==null) {
			return null;
		}
		String se = (String)msg.columnValue(field);
		if(se==null) {
			return msg;
		}
		switch (se) {
		case "1":
			return msg.with(outputfield, "Male", ImmutableMessage.ValueType.STRING);
		case "2":
			return msg.with(outputfield, "Female", ImmutableMessage.ValueType.STRING);
		case "9":
			return msg.with(outputfield, "Mixed", ImmutableMessage.ValueType.STRING);
		default:
			return msg.with(outputfield, "Unknown", ImmutableMessage.ValueType.STRING);
		}	}

}
