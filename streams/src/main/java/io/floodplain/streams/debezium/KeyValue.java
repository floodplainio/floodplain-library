package io.floodplain.streams.debezium;

public class KeyValue {
    public final String key;
    public final byte[] value;

    public KeyValue(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }
}
