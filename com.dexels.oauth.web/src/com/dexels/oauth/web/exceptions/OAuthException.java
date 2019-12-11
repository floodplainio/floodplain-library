
package com.dexels.oauth.web.exceptions;

public abstract class OAuthException extends Exception {
	private static final long serialVersionUID = -606357659059892218L;
	
	private String title;
	
	public final String getTitle() {
		return this.title;
	}
	
	public OAuthException(String title, String message) {
		super(message);
		this.title = title;
	}
}

