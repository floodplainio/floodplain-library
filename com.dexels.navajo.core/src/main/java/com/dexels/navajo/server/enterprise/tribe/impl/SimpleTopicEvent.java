package com.dexels.navajo.server.enterprise.tribe.impl;

import java.io.Serializable;

import com.dexels.navajo.server.enterprise.tribe.TopicEvent;

public class SimpleTopicEvent implements TopicEvent {

	private static final long serialVersionUID = -1683345185876275324L;
	private final Serializable myMessage;
	
	public SimpleTopicEvent(Serializable msg) {
		myMessage = msg;
	}
	
	@Override
	public Object getMessage() {
		return myMessage;
	}

	@Override
	public Object getSource() {
		return null;
	}

}