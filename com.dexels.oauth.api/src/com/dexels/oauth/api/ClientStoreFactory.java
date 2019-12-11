package com.dexels.oauth.api;

public class ClientStoreFactory {
	private static ClientStore instance = null;
	
	public static void setInstance (ClientStore store) {
		instance = store;
	}
	
	public static ClientStore getInstance () {
		return instance;
	}
}
