package com.dexels.kubernetes.elasticsearch;

import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;

@Component(name="osgi.kube.pod.elsearch"
	,configurationPolicy=ConfigurationPolicy.REQUIRE
	,service=ElasticSearchPod.class, enabled=false)

public class ElasticSearchPod {

	@Activate
	public void activate(Map<String,Object> settings) {
		System.err.println("Host: "+settings.get("host")+" port: "+settings.get("port"));
	}
	
	@Deactivate
	public void deactivate() {
		System.err.println("Shutting down");
	}
}
