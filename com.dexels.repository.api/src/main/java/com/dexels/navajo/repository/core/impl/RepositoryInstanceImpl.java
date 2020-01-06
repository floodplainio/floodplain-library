package com.dexels.navajo.repository.core.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;


public abstract class RepositoryInstanceImpl implements RepositoryInstance {
    private final static Logger logger = LoggerFactory.getLogger(RepositoryInstanceImpl.class);

	protected String repositoryName;
	protected File applicationFolder;
	protected String deployment;
	
	private final Map<String,Object> settings = new HashMap<String, Object>();
	private final Map<String,AppStoreOperation> operations = new HashMap<String, AppStoreOperation>();
	private final Map<String,Map<String,Object>> operationSettings = new HashMap<String, Map<String,Object>>();
	private ConfigurationAdmin configAdmin;
	private final Map<String,Configuration> resourcePids = new HashMap<String, Configuration>();

	protected String type;
	private Set<String> allowedProfiles;

	
	@Override
	public File getRepositoryFolder() {
		return this.applicationFolder;
	}

	// always return the applicationFolder
	@Override
	public File getOutputFolder() {
		return this.applicationFolder;
	}
	
	// always return the applicationFolder
	@Override
	public File getTempFolder() {
		return this.applicationFolder;
	}
	
	@Override
	public String getRepositoryName() {
		return repositoryName;
	}



	@Override
	public int compareTo(RepositoryInstance o) {
		return getRepositoryName().compareTo(o.getRepositoryName());
	}

	@Override
	public Map<String,Object> getSettings() {
		return settings;
	}
	
	protected void setSettings(Map<String,Object> settings) {
		this.settings.clear();
		this.settings.putAll(settings);
		String allowedProfiles = (String) settings.get("profiles");
		if(allowedProfiles!=null) {
			String[] profiles = allowedProfiles.split(",");
			Set<String> pr = new HashSet<String>();
			for (String e : profiles) {
				pr.add(e);
			}
			this.allowedProfiles = pr;
		} else {
		
		}
	}

	@Reference(cardinality=ReferenceCardinality.MULTIPLE,unbind="removeOperation",policy=ReferencePolicy.DYNAMIC)
	@Override
	public void addOperation(AppStoreOperation op, Map<String,Object> settings) {
		operations.put((String)settings.get("name"),op);
		operationSettings.put((String)settings.get("name"),settings);
	}

	@Override
	public void removeOperation(AppStoreOperation op,
			Map<String, Object> settings) {
		operations.remove(settings.get("name"));
		operationSettings.remove(settings.get("name"));
		
	}
	
	@Override
	public List<String> getOperations() {
		List<String> result = new ArrayList<String>();
		for (Map.Entry<String, AppStoreOperation> entry : operations.entrySet()) {
			String operationName = entry.getKey();
			Map<String,Object> settings = operationSettings.get(operationName);
			String operationType = (String) settings.get("type");

			if("global".equals(operationType)) {
				continue;
			}
			if("any".equals(this.type)) {
				result.add(operationName);
				continue;
			}
			
			if(operationType!=null && !this.type.equals(operationType)) {
				logger.debug("Operation type not matching: "+type+" vs. "+operationType);
				continue;
			}
			result.add(operationName);
		}
		return result;
		
	}

	
	@Override
	public String toString() {
		return getRepositoryName()+": "+repositoryType()+"=>"+applicationType();
	}

	

	protected void registerFileInstallLocations(List<String> locations) throws IOException {
		for (String location : locations) {
			File current = new File(getRepositoryFolder(),location);
			addFolderMonitorListener(current);
		}
		
	}

	private void addFolderMonitorListener(File monitoredFolder) throws IOException {
		if(!monitoredFolder.exists()) {
			logger.warn("FileInstaller should monitor folder: {} but it does not exist. Will not try again.", monitoredFolder.getAbsolutePath());
			return;
		}
		//fileInstallConfiguration = myConfigurationAdmin.createFactoryConfiguration("org.apache.felix.fileinstall",null);
//		monitoredFolder.getCanonicalFile().getAbsolutePath()
		final String absolutePath = monitoredFolder.getCanonicalFile().getAbsolutePath();
		logger.info("Adding fileinstall for folder: {}",absolutePath);
		Configuration newConfig = getUniqueResourceConfig( absolutePath);
		Dictionary<String,Object> d = newConfig.getProperties();
		if(d==null) {
			d = new Hashtable<String,Object>();
		}
		d.put("felix.fileinstall.dir",absolutePath );
		d.put("injectedBy","repository-instance" );
		d.put("felix.fileinstall.enableConfigSave", false);
		d.put("felix.fileinstall.poll", 500);
		
		String pid = newConfig.getPid();
		resourcePids.put(pid, newConfig);
//		newConfig.update(d);
		updateIfChanged(newConfig, d);
		logger.info("Adding fileinstall done for folder: {}",absolutePath);
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
	
	private Configuration getUniqueResourceConfig(String path)
			throws IOException {
		final String factoryPid = "org.apache.felix.fileinstall";
		Configuration[] cc;
		String filter = "(&(service.factoryPid=" + factoryPid
				+ ")(felix.fileinstall.dir=" + path + "))";
		try {
			cc = configAdmin.listConfigurations(filter);
		} catch (InvalidSyntaxException e) {
			logger.error("Error discovering previous fileinstalls filter: "+filter, e);
			return null;
		}
		if (cc != null) {

			if (cc.length != 1) {
				logger.info("Odd length: " + cc.length);
			}
			return cc[0];
		} else {
			logger.info("Not found: " + path+" creating a new factory config for: "+factoryPid);
			Configuration c = configAdmin.createFactoryConfiguration(
					factoryPid, null);
			return c;
		}
	}
	protected void deregisterFileInstallLocations() {
		for (Map.Entry<String, Configuration> element : resourcePids.entrySet()) {
			try {
				element.getValue().delete();
			} catch (Throwable e) {
				// usually not an issue, gives ugly errors when shutting down
				logger.debug("Problem removing fileinstalled location: ", element.getKey(),e);
			}
		}
		
	}
	
	public void setConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = configAdmin;
	}
	

	/**
	 * @param configAdmin the configAdmin to remove 
	 */
	public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = null;
	}
	

	@Override
	public String getDeployment() {
		if(deployment!=null && !"".equals(deployment)) {
			return deployment;
		}
		String envDeployment = System.getProperty("DEPLOYMENT");
		return envDeployment;
	}
	
	
	
	@Override
    public boolean requiredForServerStatus() {
        return true;
    }

    /** 
	 * Uses a ServerStatusChecker to determine whether the system is ready to receive requests again
	 */
	protected boolean postUpdateSystemCheckOK() {
	    return true;
	}

	@Override
	public Set<String> getAllowedProfiles() {
		return this.allowedProfiles ;
	}

	public Map<String,Object> getDeploymentSettings(Map<String,Object> source) {
		Map<String,Object> result = new HashMap<String, Object>();
		for (Map.Entry<String, Object> element  : source.entrySet()) {
			if(element.getKey().indexOf("/")==-1) {
				result.put(element.getKey(), element.getValue());
			} else {
				String[] parts = element.getKey().split("/");
				if(parts[0].equals(getDeployment())) {
					result.put(parts[1], element.getValue());
				} else {
					logger.info("Skipping mismatched deployment: "+parts[0]);
				}
			}
		}
		return Collections.unmodifiableMap(result);
	}
}
