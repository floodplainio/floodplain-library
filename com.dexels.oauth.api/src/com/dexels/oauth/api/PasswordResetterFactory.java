package com.dexels.oauth.api;

public class PasswordResetterFactory {
	private static PasswordResetter instance = null;
	
	public static void setInstance (PasswordResetter reset) {
		instance = reset;
	}
	
	public static PasswordResetter getInstance () {
		return instance;
	}
}
