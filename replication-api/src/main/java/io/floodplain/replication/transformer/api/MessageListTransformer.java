package io.floodplain.replication.transformer.api;

import io.floodplain.replication.api.ReplicationMessage;

import java.util.List;

// TODO not used, remove?
public interface MessageListTransformer {
    public ReplicationMessage apply(List<ReplicationMessage> input);
}
