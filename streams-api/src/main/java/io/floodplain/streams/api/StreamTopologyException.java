package io.floodplain.streams.api;

public class StreamTopologyException extends RuntimeException {

    private static final long serialVersionUID = -6299940440297210129L;

    public StreamTopologyException() {
        super();
    }

    public StreamTopologyException(String message) {
        super(message);
    }

    public StreamTopologyException(Throwable cause) {
        super(cause);
    }

    public StreamTopologyException(String message, Throwable cause) {
        super(message, cause);
    }

}
