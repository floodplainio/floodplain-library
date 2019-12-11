package com.dexels.mqtt.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name="dexels.mqtt.whiteboard")
public class MQTTWhiteboard  {
	
	private IMqttClient client;
	private ServiceTracker<MqttCallback,MqttCallback> serviceTracker;
	private final Set<MqttCallback> callbacks = new HashSet<>();
	
	private final static Logger logger = LoggerFactory.getLogger(MQTTWhiteboard.class);

	
	
	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearMQTTClient")
	public void setMQTTClient(IMqttClient client) {
		this.client = client;
	}
	
	public void clearMQTTClient(IMqttClient client) {
		this.client = null;
	}

	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="removeMqttCallback",cardinality=ReferenceCardinality.MULTIPLE)
	public void addMqttCallback(MqttCallback callback, Map<String,Object> settings) {
		callbacks.add(callback);
		try {
			String topic = (String)settings.get("topic");
			if(topic!=null) {
				client.subscribe(topic);
			}
		} catch (MqttSecurityException e) {
			logger.error("Error: ", e);
		} catch (MqttException e) {
			logger.error("Error: ", e);
		}
	}
	
	public void removeMqttCallback(MqttCallback callback, Map<String,Object> settings) {
		callbacks.remove(callback);
	}

	
	@Activate
	public void activate() {
		client.setCallback(new MqttCallback(){

			@Override
			public void connectionLost(Throwable t) {
				for (MqttCallback mqttCallback : callbacks) {
					mqttCallback.connectionLost(t);
				}
			}

			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {
				for (MqttCallback mqttCallback : callbacks) {
					mqttCallback.deliveryComplete(token);
				}
				
			}

			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				for (MqttCallback mqttCallback : callbacks) {
					mqttCallback.messageArrived(topic,message);
				}
			}});
	}

	@Deactivate
	public void deactivate() {
		if(serviceTracker!=null) {
			serviceTracker.close();
			serviceTracker = null;
		}
	}

}
