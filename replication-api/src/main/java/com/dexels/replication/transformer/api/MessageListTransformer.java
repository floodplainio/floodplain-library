package com.dexels.replication.transformer.api;

import com.dexels.replication.api.ReplicationMessage;

import java.util.List;

// TODO not used, remove?
public interface MessageListTransformer {
    public ReplicationMessage apply(List<ReplicationMessage> input);
}
