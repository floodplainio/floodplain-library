package io.floodplain.replication.factory;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessage.ValueType;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.impl.ReplicationImmutableMessageImpl;

import java.util.*;

public class ReplicationFactory {
    private static ReplicationMessageParser instance;
    private static final Runnable noopCommit = () -> {
    };


    public void setReplicationMessageParser(ReplicationMessageParser rmp) {
        setInstance(rmp);
    }


    public void clearReplicationMessageParser(ReplicationMessageParser rmp) {
        setInstance(null);
    }

    public static ReplicationMessageParser getInstance() {
        return ReplicationFactory.instance;
    }

    public static void setInstance(ReplicationMessageParser parser) {
        ReplicationFactory.instance = parser;
    }

    public void clearInstance(ReplicationMessageParser parser) {
        ReplicationFactory.instance = parser;
    }

    public static ReplicationMessage createReplicationMessage(Optional<String> source, Optional<Integer> partition, Optional<Long> offset, final String transactionId, final long timestamp,
                                                              final ReplicationMessage.Operation operation, final List<String> primaryKeys, Map<String, ValueType> types,
                                                              Map<String, Object> values, Map<String, ImmutableMessage> subMessageMap,
                                                              Map<String, List<ImmutableMessage>> subMessageListMap, Optional<Runnable> commitAction, Optional<ImmutableMessage> paramMessage) {
        return new ReplicationImmutableMessageImpl(source, partition, offset, transactionId, operation, timestamp, values, types, subMessageMap, subMessageListMap, primaryKeys, commitAction, paramMessage);
    }

    public static ReplicationMessage createReplicationMessage(Optional<String> source, Optional<Integer> partition, Optional<Long> offset, final String transactionId, final long timestamp,
                                                              final ReplicationMessage.Operation operation, final List<String> primaryKeys, ImmutableMessage message, Optional<Runnable> commitAction, Optional<ImmutableMessage> paramMessage) {
        return new ReplicationImmutableMessageImpl(source, partition, offset, transactionId, operation, timestamp, message, primaryKeys, commitAction, paramMessage);
    }

    public static ReplicationMessage fromMap(String key, Map<String, Object> values, Map<String, ValueType> types) {
        List<String> keys = key == null ? Collections.emptyList() : Arrays.asList(new String[]{key});
        return ReplicationFactory.createReplicationMessage(Optional.empty(), Optional.empty(), Optional.empty(), null, System.currentTimeMillis(), ReplicationMessage.Operation.NONE, keys, types, values, Collections.emptyMap(), Collections.emptyMap(), Optional.of(noopCommit), Optional.empty());
    }

    public static ReplicationMessage create(Map<String, Object> values, Map<String, ValueType> types) {
        return ReplicationFactory.createReplicationMessage(Optional.empty(), Optional.empty(), Optional.empty(), null, System.currentTimeMillis(), ReplicationMessage.Operation.NONE, Collections.emptyList(), types, values, Collections.emptyMap(), Collections.emptyMap(), Optional.of(noopCommit), Optional.empty());
    }


    public static ReplicationMessage create(Map<String, Object> dataMap) {
        return new ReplicationImmutableMessageImpl(dataMap);
    }

    public static ReplicationMessage empty() {
        return ReplicationFactory.createReplicationMessage(Optional.empty(), Optional.empty(), Optional.empty(), null, System.currentTimeMillis(), ReplicationMessage.Operation.NONE, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Optional.of(noopCommit), Optional.empty());
    }

    public static ReplicationMessage joinReplicationMessage(String key, ReplicationMessage a, ReplicationMessage b) {
        return new ReplicationImmutableMessageImpl(a, b, key);
    }

    public static ReplicationMessage createErrorReplicationMessage(Throwable t) {
        return new ReplicationImmutableMessageImpl(t);
    }

    public static ReplicationMessage standardMessage(ImmutableMessage msg) {
        return new ReplicationImmutableMessageImpl(Optional.<String>empty(), Optional.empty(), Optional.empty(), (String) null, ReplicationMessage.Operation.NONE, -1L, msg, Collections.<String>emptyList(), Optional.empty(), Optional.empty());
    }


}