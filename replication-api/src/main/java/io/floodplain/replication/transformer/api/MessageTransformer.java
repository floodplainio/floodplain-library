package io.floodplain.replication.transformer.api;

import io.floodplain.replication.api.ReplicationMessage;

import java.util.Map;

public interface MessageTransformer {
    public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg);
}
