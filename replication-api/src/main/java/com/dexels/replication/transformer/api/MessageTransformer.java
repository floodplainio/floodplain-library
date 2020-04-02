package com.dexels.replication.transformer.api;

import com.dexels.replication.api.ReplicationMessage;

import java.util.Map;

public interface MessageTransformer {
    public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg);
}
