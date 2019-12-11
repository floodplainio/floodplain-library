package com.dexels.mqtt.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class MqttClientTest  {

	private AtomicInteger received = new AtomicInteger();

    @Ignore
	@Test
    public void testConnection() throws Exception {
		MQTTClientRegistrator mcr = new MQTTClientRegistrator();
		Map<String,Object> settings = new HashMap<>();
		settings.put("uri", "tcp://localhost:1883");
		settings.put("clientId", "junit");
		mcr.activate(settings, null);
		MQTTWhiteboard mw = new MQTTWhiteboard();
		IMqttClient client = mcr.getClient();
		mw.setMQTTClient(client);
		mw.activate();
		mw.addMqttCallback(new MqttCallback() {
			
			@Override
			public void messageArrived(String topic, MqttMessage msg) throws Exception {
				System.err.println("message received on topic: "+topic+" total: "+received.incrementAndGet());
			}
			
			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {
				System.err.println("delivery complete!");
			}
			
			@Override
			public void connectionLost(Throwable t) {
				t.printStackTrace();
			}
		},new HashMap<String,Object>());
		client.subscribe("aap/noot");
		client.subscribe("zus/+");
		
		client.publish("aap/noot", "blablabla".getBytes(), 1, false);
		Thread.sleep(500);
		Assert.assertEquals(1, received.intValue());
		client.publish("aap/mies", "blablabla".getBytes(), 1, false);
		Thread.sleep(500);
		Assert.assertEquals(1, received.intValue());
		client.publish("zus/jet", "blablabla".getBytes(), 1, false);
		Thread.sleep(500);
		Assert.assertEquals(2, received.intValue());
		
		System.err.println("done");
	}
}
