package com.dexels.hazelcast.kubernetes;

public interface Event {

    public String uid();

    public String message();

    public String reason();

    public String type();

    public String namespace();

    public String source();
}
