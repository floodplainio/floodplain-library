package com.dexels.mqtt.impl;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name="dexels.mqtt.listenall",property={"topic=#"},configurationPolicy=ConfigurationPolicy.REQUIRE)
public class MQTTListenAll implements MqttCallback {

	
	private final static Logger logger = LoggerFactory.getLogger(MQTTListenAll.class);

	
	@Override
	public void connectionLost(Throwable e) {
		logger.info("Connection lost: ",e);
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		logger.info("deliviery complete: "+token);

	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		logger.info("Message on topic: "+topic+" arrived, message: "+new String(message.getPayload()));
	}

}
