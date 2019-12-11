package com.dexels.hazelcast.kubernetes.impl;

import com.dexels.hazelcast.kubernetes.Event;

public class EventImpl implements Event {

    private final String uid;
    private final String message;
    private final String reason;
    private final String type;
    private final String namespace;
    private final String source;

    public EventImpl(String uid, String message, String reason, String type, String namespace, String source) {
        this.uid = uid;
        this.message = message;
        this.reason = reason;
        this.type = type;
        this.namespace = namespace;
        this.source = source;
    }

    @Override
    public String uid() {
        return uid;
    }

    @Override
    public String message() {
        return message;
    }

    @Override
    public String reason() {
        return reason;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public String source() {
        return source;
    }

}
