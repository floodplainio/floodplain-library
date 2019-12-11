package com.dexels.navajo.tipi.dev.server.websocket.impl;

import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
 
public class CallbackSocketCreator implements WebSocketCreator {
 
 
    private final CallbackServlet servlet;

	public CallbackSocketCreator(CallbackServlet servlet) {
    	this.servlet = servlet;
    }
 
    @Override
    public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
    	return new SCSocket(servlet);
    }
}