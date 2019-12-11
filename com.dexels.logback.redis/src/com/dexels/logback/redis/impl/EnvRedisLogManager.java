package com.dexels.logback.redis.impl;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name="dexels.redis.client.env")
public class EnvRedisLogManager {
	
	private ConfigurationAdmin configAdmin;
	
	private final static Logger logger = LoggerFactory
			.getLogger(EnvRedisLogManager.class);

	private Configuration configuration;

	private static final int DEFAULT_PORT = 6379;

	@Activate
	public void activate() throws IOException {
		Map<String,String> env = System.getenv();
		
		String host = getenv(env,"REDIS_LOG_HOST");
		String port = getenv(env,"REDIS_LOG_PORT");
//		String dedot = env.get("REDIS_DEDOT");
		String suppress = env.get("NO_REDIS");
		
		
		if(suppress!=null) {
			logger.warn("Explicit NO_REDIS flag set, suppressing log output to redis!");
			return;
		}
		boolean dedotKeys = true; // dedot!=null;
		
		if (host == null) {
            host = getenv(env, "HOSTIP");
        }
		
		if(port==null) {
			port =  ""+DEFAULT_PORT;
		}
		if(host!=null) {
			injectConfig(host,port,dedotKeys);
			return;
		}
//		REDIS_PORT=tcp://172.17.0.121:6379
		String envkey = "REDIS_PORT";
		if(env.get(envkey)==null) {
			logger.warn("No REDIS_PORT defined in environment, so no redis configurations will be injected.");
			return;
		}
		String[] hosturl = extractUrl(env.get(envkey));
		System.err.println("Trying to extract destination from env: "+envkey+" resulting in: "+env.get(envkey));
		if(hosturl!=null) {
			injectConfig(hosturl[0], hosturl[1],dedotKeys);
			logger.info("Injected container linking based config. Host: {} post: {}",hosturl[0], hosturl[1]);
		} else {
			logger.warn("No 'REDIS_LOG_HOST' set, so no redis configurations will be injected.");
		}
	}

	private String getenv(Map<String, String> env, String key) {
		String result = env.get(key);
		if(result!=null) {
			return result;
		}
		return System.getProperty(key);
	}



	private void injectConfig(String host, String port, boolean deDotKeys) throws IOException {
		Configuration c = createOrReuse("dexels.redis.client", "(name=dexels.redis.client.env)",false);
		Dictionary<String,Object> properties = new Hashtable<String,Object>();
		properties.put("name", "dexels.redis.client.env");
		properties.put("host", host);
		properties.put("port", port);
		properties.put("dedot", deDotKeys);
		c.update(properties);	
	}

	private Configuration createOrReuse(String pid, final String filter, boolean factory)
			throws IOException {
		configuration = null;
		try {
			Configuration[] c = configAdmin.listConfigurations(filter);
			if(c!=null && c.length>1) {
				logger.warn("Multiple configurations found for filter: {}", filter);
			}
			if(c!=null && c.length>0) {
				configuration = c[0];
			}
		} catch (InvalidSyntaxException e) {
			logger.error("Error in filter: {}",filter,e);
		}
		if(configuration==null) {
			if (factory) {
				configuration = configAdmin.createFactoryConfiguration(pid,null);
			} else {
				configuration = configAdmin.getConfiguration(pid,null);
			}
		}
		return configuration;
	}
	
	
	@Deactivate
	public void deactivate() {
		try {
			if(configuration!=null) {
				configuration.delete();
			}
		} catch (IOException e) {
			logger.error("Error: ", e);
		}
		
		
	}
	@Reference(name="ConfigAdmin", unbind="clearConfigAdmin")
	public void setConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = configAdmin;
	}

	/**
	 * @param configAdmin the configAdmin to remove 
	 */
	public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = null;
	}

	public static void main(String[] args) {
		String input = "tcp://172.17.0.5:5432";
		System.err.println("host: "+extractUrl(input)[0]);
		System.err.println("port: "+extractUrl(input)[1]);
	}

	private static String[] extractUrl(String input) {
		String[] ii = input.split("tcp://");
		if(ii.length==0) {
			return null;
		}
		String content = ii[1];
		String[] parts = content.split(":");
		if(parts.length==2) {
			return parts;
		}
		return null;
	}
}
