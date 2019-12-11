package com.dexels.hazelcast.kubernetes;

public class DiscoveryException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 386190637574050356L;

	public DiscoveryException(String message) {
		super(message);
	}
	
	public DiscoveryException(String message, Throwable cause) {
		super(message,cause);
	}
}
