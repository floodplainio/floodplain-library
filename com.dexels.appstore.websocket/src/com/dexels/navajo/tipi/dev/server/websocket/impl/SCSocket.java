package com.dexels.navajo.tipi.dev.server.websocket.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.tipi.dev.server.appmanager.TipiCallbackSession;

public class SCSocket extends WebSocketAdapter implements TipiCallbackSession {

	private final CallbackServlet callbackServlet;
	private Session connection;
	private boolean initialized = false;
	private ServiceRegistration<TipiCallbackSession> registration;
	private String application;
	private String tenant;
	private String session;

	private final static Logger logger = LoggerFactory
			.getLogger(SCSocket.class);
	
	/**
	 * @param callbackServlet
	 */
	public SCSocket(CallbackServlet callbackServlet) {
		this.callbackServlet = callbackServlet;
	}

	@Override
	public void onWebSocketBinary(byte[] payload, int offset, int len) {
		//super.onWebSocketBinary(payload, offset, len);
		byte[] copy = Arrays.copyOfRange(payload, offset, offset+len);
		logger.info("binary data received over websocket: "+new String(copy));
	}

	@Override
	public void onWebSocketClose(int statusCode, String reason) {
		this.callbackServlet.removeSocket(this);
		if (registration != null) {
			registration.unregister();
			registration = null;
		}
	}

	@Override
	public void onWebSocketConnect(Session sess) {
		this.connection = sess;
		logger.info("WebSocket connect received");
		this.callbackServlet.addSocket(this);
		RemoteEndpoint remote = sess.getRemote();
		try {
			remote.sendString("You have been connected");
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
	}

	@Override
	public void onWebSocketError(Throwable cause) {
		logger.info("WebSocket error ",cause);
		this.callbackServlet.removeSocket(this);
		if (registration != null) {
			registration.unregister();
			registration = null;
		}
	}

	@Override
	public void onWebSocketText(String message) {
		System.out.println("Received: " + message);
		logger.info("Websocket test received: {}",message);
		if (!initialized) {
			registerService(message);
			initialized = true;
		}
	}

	private void registerService(String initialMessage) {
		String[] elements = initialMessage.split(";");
		Dictionary<String, Object> settings = new Hashtable<String, Object>();
		this.application = elements[0];
		this.tenant = elements[1];
		this.session = elements[2];
		settings.put("application", application);
		settings.put("tenant", tenant);
		settings.put("session", session);
		registration = callbackServlet.getBundleContext().registerService(
				TipiCallbackSession.class, this, settings);

	}

	public boolean isOpen() {
		return connection.isOpen();
	}

	@Override
	public String getApplication() {
		return application;
	}

	@Override
	public String getProfile() {
		return tenant;
	}

	@Override
	public String getSessionId() {
		return session;
	}

	@Override
	public void sendMessage(String data) throws IOException {
		RemoteEndpoint remote = connection.getRemote();
		try {
			remote.sendString(data);
		} catch (IOException e) {
			logger.error("Error: ", e);
		}
	}

	@Override
	public Map<String, String> toMap() {
		final Map<String,String> result = new HashMap<>();
		result.put("application", application);
		result.put("tenant", tenant);
		result.put("sessionId", session);
		return result;
	}
}