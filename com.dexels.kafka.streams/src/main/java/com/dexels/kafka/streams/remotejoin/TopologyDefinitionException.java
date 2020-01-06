package com.dexels.kafka.streams.remotejoin;

public class TopologyDefinitionException extends RuntimeException {

	private static final long serialVersionUID = -7329418791031646069L;

	public TopologyDefinitionException(String message, Throwable cause) {
		super(message, cause);
	}

	public TopologyDefinitionException(String message) {
		super(message);
	}

	public TopologyDefinitionException(Throwable cause) {
		super(cause);
	}

	
}
