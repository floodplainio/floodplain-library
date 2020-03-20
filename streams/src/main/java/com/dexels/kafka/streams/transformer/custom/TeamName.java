package com.dexels.kafka.streams.transformer.custom;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

import java.util.Map;

public class TeamName implements MessageTransformer {

	@Override
	public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
		String reportingName = (String) msg.columnValue("ReportingName");
		if(reportingName!=null) {
			return msg.with("TeamName", reportingName, ImmutableMessage.ValueType.STRING);
		}
		return msg.with("TeamName", ""+msg.columnValue("ClubName")+" "+msg.columnValue("TeamCode"), ImmutableMessage.ValueType.STRING);
	}

}
