package com.dexels.hazelcast.kubernetes.type;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

@Component(name="dexels.cluster.discovery.navajo", configurationPolicy=ConfigurationPolicy.REQUIRE)
public class NavajoClusterMemberService {

	private static final String PATH = "/status";
	@SuppressWarnings("unused")
	@Activate
	public void activate(Map<String,Object> settings) {
		String portsString = (String) settings.get("ports");
		String ip = (String) settings.get("ip");
		List<String> ports = Arrays.asList(portsString.split(","));
		String defaultPort = ports.get(0);
		String url = "http://"+ip+":"+defaultPort+PATH;
	}
}
