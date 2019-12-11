package com.dexels.navajo.repository.file.impl;

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

@Component(name="dexels.repository.file.env")
public class EnvFileRepositoryManager {
	
	private ConfigurationAdmin configAdmin;
	
	private final static Logger logger = LoggerFactory
			.getLogger(EnvFileRepositoryManager.class);

	private Configuration configuration;
	
	@Activate
	public void activate() throws IOException {
		Map<String,String> env = System.getenv();
		String path = env.get("FILE_REPOSITORY_PATH");
		String type = env.get("FILE_REPOSITORY_TYPE");
		String deployment = env.get("FILE_REPOSITORY_DEPLOYMENT");
		if(path==null) {
			logger.debug("No 'FILE_REPOSITORY_PATH' set, so no file repositories will be injected.");
			return;
		}
		if(type==null) {
			logger.warn("No 'FILE_REPOSITORY_TYPE' set, so no file repositories will be injected.");
			return;
		}
		if(deployment==null) {
			logger.warn("No 'FILE_REPOSITORY_DEPLOYMENT' set, so no file repositories will be injected.");
			return;
		}
		injectConfig(path,type,deployment,env);
	}
	


	private void injectConfig(String path, String type, String deployment, Map<String,String> env) throws IOException {
		Configuration c = createOrReuse("dexels.repository.file", "(repository.name=env.managed.repository)");
		Dictionary<String,Object> properties = new Hashtable<String,Object>();
		properties.put("repository.type", type);
		properties.put("type", type);
		properties.put("repository.name", "env.managed.repository");
		properties.put("repository.folder", path);
		properties.put("repo", "file");
		if(deployment!=null) {
			properties.put("repository.deployment", deployment);
		}
		String fileinstall = env.get("FILE_REPOSITORY_FILEINSTALL");
		if(fileinstall!=null) {
			properties.put("fileinstall", fileinstall);
		}
		String monitored = env.get("FILE_REPOSITORY_MONITORED");
		if(monitored!=null) {
			properties.put("monitored", monitored);
		}
		c.update(properties);	
		Configuration manager =  configAdmin.getConfiguration("navajo.repository.manager",null);
		Dictionary<String,Object> managerProperties = new Hashtable<String,Object>();
        String storagePath = env.get("FILE_REPOSITORY_STORAGE");
        if (storagePath != null && !"".equals(storagePath)) {
            managerProperties.put("storage.output", storagePath);
            managerProperties.put("storage.temp", storagePath);
            managerProperties.put("storage.path", storagePath);
        } else {
            managerProperties.put("storage.output", System.getProperty("java.io.tmpdir"));
            managerProperties.put("storage.temp", System.getProperty("java.io.tmpdir"));
            managerProperties.put("storage.path", System.getProperty("java.io.tmpdir"));
        }

		manager.update(managerProperties);
	}

	private Configuration createOrReuse(String pid, final String filter)
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
			configuration = configAdmin.createFactoryConfiguration(pid,null);
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

}
