package com.dexels.navajo.tipi.dev.server.appmanager;

import java.io.IOException;
import java.util.Map;

public interface TipiCallbackSession {

	public void sendMessage(String data) throws IOException;

	public String getApplication();
	
	public String getProfile();
	public String getSessionId();
	
	public Map<String,String> toMap();
}
