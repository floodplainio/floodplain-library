package com.dexels.hazelcast.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;

@Component(name="dexels.hazelcast.config",service={Config.class,HazelcastOSGiConfig.class},immediate=true,configurationPolicy=ConfigurationPolicy.REQUIRE)
public class HazelcastOSGiConfig extends Config implements Serializable {

	private static final long serialVersionUID = 1107113552512858013L;
	
	private static final Logger logger = LoggerFactory.getLogger(HazelcastOSGiConfig.class);

	private Integer clusterPort;
	private String clusterName;
	private String[] clusterMembers;
	private String instanceName;
	private String uniqueName;
	private String hostName;
	private Integer externalPort;
	private String localInterface;


	@Activate
	public void activate(Map<String,Object> settings) {
		try {
			if(settings!=null) {
				clusterPort = (Integer) settings.get("cluster.port");
				clusterName = (String) settings.getOrDefault("cluster.name","defaultCluster");
				instanceName = (String) settings.getOrDefault("cluster.instance","defaultInstanceName");
				clusterMembers = (String[]) settings.get("cluster.members");
				hostName = (String) settings.get("cluster.hostName");
				externalPort = (Integer) settings.get("cluster.externalPort");
				localInterface = (String) settings.get("cluster.interface");
				uniqueName = clusterName+"/"+instanceName;
				if(externalPort==null) {
					externalPort = 5701;
				}
				if(clusterPort!=null) {
					getNetworkConfig().setPort(clusterPort);
				}
				for (Entry<String,Object> e : settings.entrySet()) {
					if(e.getKey().startsWith("hazelcast.")) {
						setProperty(e.getKey(),
								  (String)e.getValue());
					}
				}
			}
			
		
			setProperty("hazelcast.logging.type", "slf4j");
			getGroupConfig().setName(clusterName);
			setInstanceName(uniqueName);
			
			getMapConfig("default").setBackupCount(1);
			getMapConfig("default").setAsyncBackupCount(1);
			
			if (clusterMembers==null) {
				configureMulticast();
			} else {
				configureIp();
			}

		} catch (Exception e) {
			logger.error("Hazelcast config error: ", e);
		}
	}

	private void configureMulticast() {
		logger.info("Configuring multicast");
		getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
		getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
		getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
		if (localInterface != null) {
			logger.info("Binding to: {}",localInterface);
			setProperty("hazelcast.socket.bind.any", "false");
			getNetworkConfig().getInterfaces().setEnabled(true).addInterface(localInterface);
		} else {
			logger.info("No specific interfaces");
		}
		getNetworkConfig().setPortAutoIncrement(true);
	}
	
	private void configureIp() {
		logger.info("Configuring ip. Public address: {} port: {} length: {}",hostName, externalPort,clusterMembers.length);
		NetworkConfig networkConfig = getNetworkConfig();
		networkConfig.setPortAutoIncrement(false);
		networkConfig.getJoin().getMulticastConfig().setEnabled(false);
		TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
		tcpIpConfig.setEnabled(true);
		List<String> members = new ArrayList<>();
	    networkConfig.setPublicAddress(hostName+":"+externalPort);
	    if (localInterface != null) {
	        networkConfig.getInterfaces().setEnabled(true).addInterface(localInterface);
	    }

		for (String member : clusterMembers) {
		    members.add(member);
		}
	    tcpIpConfig.setMembers(members);

	}
	
	@Override
	public String getInstanceName() {
		return instanceName;
	}
	
	public String getUniqueName() {
		return uniqueName;
	}
	
	public int getClusterSize() {
        return clusterMembers.length;
    }

}
