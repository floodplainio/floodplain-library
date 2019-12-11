package com.dexels.hazelcast.kubernetes.impl;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.kubernetes.DiscoverClusterMembers;

@Component(name="dexels.hazelcast.topology.kubernetes",configurationPolicy=ConfigurationPolicy.OPTIONAL)
public class HazelcastTopologyInjector {
	
	
	private static final String DEXELS_HAZELCAST_CONFIG_PID = "dexels.hazelcast.config";

	public static final int CLUSTER_PORT = 5701; 
	private ConfigurationAdmin configAdmin;
    private ServiceRegistration<ClassLoader> classLoaderService;

	private Configuration config;

	private DiscoverClusterMembers discoverClusterMembers;

	private final static Logger logger = LoggerFactory.getLogger(HazelcastTopologyInjector.class);

	@Activate
	public void activate(Map<String,Object> settings, BundleContext bundleContext) throws IOException {
		Map<String,String> env = System.getenv();
		String cluster = getenv(env, "CLUSTER");
		String instance = getenv(env, "INSTANCENAME");
		String host = getenv(env, "HOSTNAME");

		if(cluster==null) {
			logger.warn("No CLUSTER defined, ignoring further Hazelcast topologies");
			return;
		}
		String namespace = discoverClusterMembers.currentNamespace();
		if(namespace == null  ) {
			logger.warn("No namespace provided for kube. Not injecting kube based hazelcast");
			return;
		}
		String hostip = getenv(env, "HOSTIP") !=null ?  getenv(env, "HOSTIP") : Inet4Address.getLocalHost().getHostAddress();
		if(host == null) {
			host = hostip;
		}
		String clusterPort = getenv(env, "CLUSTER_PORT")==null?"5701":getenv(env, "CLUSTER_PORT");
		Boolean useDefaultClassloader = false;
		String defaultClassloaderString = getenv(env, "CLUSTER_USE_DEFAULT_CLASSLOADER");
		if (defaultClassloaderString != null) {
		    useDefaultClassloader = Boolean.valueOf(defaultClassloaderString);
		}
		if (useDefaultClassloader) {
            Dictionary<String,Object> classsettings = new Hashtable<String,Object>();
            classsettings.put("tag", "hazelcast");
            classLoaderService = bundleContext.registerService(ClassLoader.class, getClass().getClassLoader(), classsettings);
        }
		configure(namespace,cluster, instance,hostip, clusterPort==null?5701:Integer.parseInt(clusterPort));
		
          
	}

	private void configure(String namespace, String clusterName, String instanceName, String hostip, int clusterPort) throws IOException {
		try {
			config = createOrReuse(DEXELS_HAZELCAST_CONFIG_PID, "(source=kubernetes)");
//			config = configAdmin.getConfiguration(DEXELS_HAZELCAST_CONFIG_PID,null);
			Dictionary<String, Object> settings = createSettings(namespace, clusterName,
					instanceName, clusterPort,hostip);
			if(!settings.isEmpty()) {
				updateIfChanged(config, settings);
				config.update(settings);
			}
		} catch (Exception e) {
			logger.error("Error: ", e);
		}
	}


	private Configuration createOrReuse(String pid, final String filter)
			throws IOException {
		Configuration cc = null;
		try {
			Configuration[] c = configAdmin.listConfigurations(filter);
			if(c!=null && c.length>1) {
				logger.warn("Multiple configurations found for filter: {}", filter);
			}
			if(c!=null && c.length>0) {
				cc = c[0];
			}
		} catch (InvalidSyntaxException e) {
			logger.error("Error in filter: {}",filter,e);
		}
		if(cc==null) {
			cc = configAdmin.createFactoryConfiguration(pid,null);
//			resourcePids.add(cc.getPid());
		}
		return cc;
	}
	
    private void updateIfChanged(Configuration c, Dictionary<String,Object> settings) throws IOException {
		Dictionary<String,Object> old = c.getProperties();
		if(old!=null) {
			if(!old.equals(settings)) {
				c.update(settings);
			}
		} else {
			c.update(settings);
		}
	}


	@Deactivate
    public void deactivate() {
        if (config != null) {
            try {
                config.delete();
            } catch (IOException e) {
                logger.error("Error: ", e);
            }
        }
        if (classLoaderService != null) {
            classLoaderService.unregister();
        }

    }


	
	
	@Reference(unbind="clearConfigAdmin",policy=ReferencePolicy.DYNAMIC)
	public void setConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = configAdmin;
	}

	/**
	 * @param configAdmin the configAdmin to remove 
	 */
	public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = null;
	}
	
	
	@Reference(unbind="clearDiscoverClusterMembers",policy=ReferencePolicy.DYNAMIC)
	public void setDiscoverClusterMembers(DiscoverClusterMembers dcm) {
		this.discoverClusterMembers = dcm;
	}

	public void clearDiscoverClusterMembers(DiscoverClusterMembers dcm) {
		this.discoverClusterMembers = null;
	}

	
	private List<String> getClusterMembers(String namespace, String cluster) throws IOException {
		return this.discoverClusterMembers.discoverPods(namespace, cluster, CLUSTER_PORT);
	}


	
	private Dictionary<String, Object> createSettings(String namespace, String clusterName, String instanceName, int clusterPort, String hostIp)
			throws IOException {
		Dictionary<String, Object> settings = new Hashtable<String, Object>();
		if(namespace==null) {
			logger.warn("No namespace for hazelcast k8s, ignoring config");
			return settings;
		}
		if(clusterName==null) {
			logger.warn("No cluster name for hazelcast k8s, ignoring config");
			return settings;
		}
		settings.put("cluster.name", clusterName);
		settings.put("cluster.instance",  instanceName + "@" + hostIp);
		settings.put("cluster.port", clusterPort);
		settings.put("cluster.externalport", clusterPort);
		settings.put("cluster.hostName", hostIp);
		List<String> list =  getClusterMembers(namespace, clusterName); 
		logger.info("Creating topology for cluster: {} instance: {} port: {} hostIp: {} members: {}",clusterName,instanceName,clusterPort,hostIp,list);
		String[] ss = new String[list.size()];
		int index = 0;
		for (String e : list) {
			ss[index++] = e;
		}
		settings.put("cluster.members", ss);
		return settings;
	}
	
	
	private String getenv(Map<String, String> env, String key) {
		String result = env.get(key);
		if(result!=null) {
			return result;
		}
		return System.getProperty(key);
	}

}
