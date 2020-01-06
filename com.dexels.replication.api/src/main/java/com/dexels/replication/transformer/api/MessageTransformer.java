package com.dexels.replication.transformer.api;

import java.util.Map;

import com.dexels.replication.api.ReplicationMessage;

public interface MessageTransformer {
	public ReplicationMessage apply(Map<String,String> params, ReplicationMessage msg);
}
