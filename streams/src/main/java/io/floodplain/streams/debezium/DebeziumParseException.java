package io.floodplain.streams.debezium;

public class DebeziumParseException extends Exception {
    public DebeziumParseException(String message) {
        super(message);
    }

    public DebeziumParseException(String message, Throwable cause) {
        super(message,cause);
    }
}
