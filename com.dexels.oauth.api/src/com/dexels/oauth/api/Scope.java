package com.dexels.oauth.api;

public interface Scope {
	public String getId();
	public String getDescription();
	public String getTitle();
	public boolean isOptional();
	public boolean isOpenIDScope();
}
