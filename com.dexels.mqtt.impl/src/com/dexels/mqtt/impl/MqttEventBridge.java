package com.dexels.mqtt.impl;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;

@Component(name="dexels.mqtt.eventbridge")
public class MqttEventBridge implements MqttCallback {
	private EventAdmin eventAdmin = null;

	public void activate(Map<String,Object> settings) {

	}

	@Override
	public void connectionLost(Throwable arg0) {
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		
	}

	@Reference(unbind="clearEventAdmin",policy=ReferencePolicy.DYNAMIC)
	public void setEventAdmin(EventAdmin eventAdmin) {
		this.eventAdmin = eventAdmin;
	}
	
	public void clearEventAdmin(EventAdmin eventAdmin) {
		this.eventAdmin = null;
	}
	
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		Map<String,Object> properties = new HashMap<>();
		properties.put("payload", message.getPayload());
		properties.put(topic, message);
		eventAdmin.postEvent(new Event(topic,properties));
	}
}
