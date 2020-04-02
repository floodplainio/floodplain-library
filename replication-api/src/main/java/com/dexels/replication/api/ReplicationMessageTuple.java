package com.dexels.replication.api;

public class ReplicationMessageTuple {
    public final ReplicationMessage first;
    public final ReplicationMessage second;

    public ReplicationMessageTuple(ReplicationMessage first, ReplicationMessage second) {
        this.first = first;
        this.second = second;
    }
}
