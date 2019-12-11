package com.dexels.oauth.web.exceptions;

public class OAuthLoginRequiredException extends OAuthException {
	private static final long serialVersionUID = -3098784782972170552L;

	public OAuthLoginRequiredException() {
		super("login_required", "The Authorization Server requires End-User authentication.");
	}	
}
