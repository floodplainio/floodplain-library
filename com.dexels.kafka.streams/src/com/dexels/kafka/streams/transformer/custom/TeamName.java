package com.dexels.kafka.streams.transformer.custom;

import java.util.Map;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

public class TeamName implements MessageTransformer {

	@Override
	public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
		String reportingName = (String) msg.columnValue("ReportingName");
		if(reportingName!=null) {
			return msg.with("TeamName", reportingName, "string");
		}
		return msg.with("TeamName", ""+msg.columnValue("ClubName")+" "+msg.columnValue("TeamCode"), "string");
	}

}
