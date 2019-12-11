package com.dexels.kafka.streams.api;

public class StreamTopologyException extends RuntimeException {

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
