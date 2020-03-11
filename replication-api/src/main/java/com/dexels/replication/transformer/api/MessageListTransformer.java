package com.dexels.replication.transformer.api;

import java.util.List;

import com.dexels.replication.api.ReplicationMessage;

// TODO not used, remove?
public interface MessageListTransformer {
	public ReplicationMessage apply(List<ReplicationMessage> input);
}
