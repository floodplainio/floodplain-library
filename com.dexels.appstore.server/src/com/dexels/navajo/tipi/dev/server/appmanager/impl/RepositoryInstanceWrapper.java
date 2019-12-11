package com.dexels.navajo.tipi.dev.server.appmanager.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.tipi.dev.core.projectbuilder.Dependency;
import com.dexels.navajo.tipi.dev.server.appmanager.TipiCallbackSession;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RepositoryInstanceWrapper implements RepositoryInstance {
	
	private final List<Dependency> dependencies = new ArrayList<Dependency>();
	private final RepositoryInstance instance;
	private List<String> profiles = new ArrayList<String>();
	private transient PropertyResourceBundle settings;
	private Map<String, TipiCallbackSession> sessions;
	
	private final static Logger logger = LoggerFactory.getLogger(RepositoryInstanceWrapper.class);
	
	
	public final static String STATUS_MISSING = "MISSING";
	public final static String STATUS_OK = "OK";
	public final static String STATUS_OUTDATED = "OUTDATED";

	
	public RepositoryInstanceWrapper(RepositoryInstance instance) {
		this.instance = instance;
		try {
			load();
		} catch (IOException e) {
			logger.error("Error: ", e);
		}
	}

	
	public Map<String,String> getProfileStatus() {
		Map<String,String> result = new HashMap<String, String>();
		if(profiles == null) {
			return result;
		}
		for (String profile: profiles) {
			result.put(profile, profileStatus(profile));
		}
		return result;
	}
	
	public boolean getServerStatusIsOk() {
//	    if (!repositoryType().equals("github")) {
//	        return true;   // only check for github applications
//	    }
	    if (!applicationType().equals("webstart")) {
	        return true;   // only check for webstart applications
	    }
	    if (!requiredForServerStatus()) {
	        return true;   // This repo is not required for server status
	    }
	    return isBuilt();
	}

	public boolean isBuilt() {
		Map<String,String> result = getProfileStatus();
		if(result.isEmpty()) {
			return false;
		}
		if(result.values().contains(STATUS_OUTDATED)) {
			return false;
		}
		if(result.values().contains(STATUS_MISSING)) {
			return false;
		}
		return true;
		
	}
	
	private String profileStatus(String name) {
		File jnlp = new File(instance.getRepositoryFolder(),name+".jnlp");
		if(!jnlp.exists()) {
			return STATUS_MISSING;
		}
		File applicationProperties = new File(instance.getRepositoryFolder(), "settings/tipi.properties");
		File tipiSettings = new File(instance.getRepositoryFolder(), "settings/tipi.properties");
		File profileFile = new File(instance.getRepositoryFolder(),"settings/profiles/"+name+".properties");
		long jnlpModified = jnlp.lastModified();
		if(applicationProperties.lastModified()>=jnlpModified) {
			return STATUS_OUTDATED;
		}
		if(tipiSettings.lastModified()>=jnlpModified) {
			return STATUS_OUTDATED;
		}
		if(profileFile.lastModified()>=jnlpModified) {
			return STATUS_OUTDATED;
		}
		return STATUS_OK;
	}

	
	public List<String> getProfiles() {
		return profiles;
	}
	
    private PropertyResourceBundle readBundle(File f) throws FileNotFoundException, IOException {
    	if(f==null || !f.exists()) {
    		return null;
    	}
    	try(FileInputStream fis = new FileInputStream(f)) {
    		InputStreamReader isr = new InputStreamReader(fis,Charset.forName("UTF-8"));
    		return new PropertyResourceBundle(isr);
    	}
    }
	public void load() throws IOException {
		dependencies.clear();
		File tipiSettings = new File(instance.getRepositoryFolder(), "settings/tipi.properties");
		if (!tipiSettings.exists()) {
			return;
		}
		settings = readBundle(tipiSettings);
		processProfiles();
		
		String deps;
		try {
			deps = getSettingString("dependencies");
			String[] d = deps.split(",");
			for (String dependency : d) {
				
				Dependency dd = new Dependency(dependency);
				dependencies.add(dd);
			}
		} catch (MissingResourceException e) {
			logger.error("No 'dependencies' setting found in application: "+instance.getRepositoryName(), e);
		}

	}
	
	public String getSettingString(String key) {
		return settings.getString(key);
	}

	
	public List<Dependency> getDependencies() {
		return dependencies;
	}

	@JsonIgnore
	public ResourceBundle getSettingsBundle() {
		return settings;
	}

	private void processProfiles() {
		List<String> pro = new LinkedList<String>();
		File profilesDir = new File(instance.getRepositoryFolder(), "settings/profiles");

		if (profilesDir.exists()) {
			for (File file : profilesDir.listFiles()) {
				if (file.canRead() && file.isFile()
						&& file.getName().endsWith(".properties")) {
					String profileName = file.getName().substring(0,
							file.getName().length() - ".properties".length());
					Set<String> allowedProfiles = getAllowedProfiles();
					if(allowedProfiles!=null) {
						if(allowedProfiles.contains(profileName)) {
							pro.add(profileName);
						}
					} else {
						// if no allowed profiles are specified, add all of them
						pro.add(profileName);
					}
//					if(isApplicableProfile(file)) {
//					}
				}
			}
		}
		this.profiles = pro;
	}

	@Override
	public int compareTo(RepositoryInstance o) {
	    if (requiredForServerStatus() != o.requiredForServerStatus()) {
	        return -Boolean.compare(requiredForServerStatus(), o.requiredForServerStatus());
	    } 
		return instance.getRepositoryName().compareTo(o.getRepositoryName());
	}


	@Override
	public String getRepositoryName() {
		return instance.getRepositoryName();
	}


	@Override
    public boolean requiredForServerStatus() {
        return instance.requiredForServerStatus();
    }

	@Override
	public File getRepositoryFolder() {
		return instance.getRepositoryFolder();
	}


	@Override
	public Map<String, Object> getSettings() {
		Map<String, Object> intermediate = instance.getSettings();
		if(intermediate==null) {
			return null;
		}
		Object type = intermediate.get("repository.type");
		intermediate.put("type", type);
		return intermediate;
	}


	@Override
	public void addOperation(AppStoreOperation op, Map<String, Object> settings) {
		instance.addOperation(op, settings);
	}


	@Override
	public void removeOperation(AppStoreOperation op,
			Map<String, Object> settings) {
		instance.removeOperation(op, settings);
		
	}


	@Override
	public List<String> getOperations() {
		return instance.getOperations();
	}


	@Override
	public void refreshApplication() throws IOException {
		instance.refreshApplication();
	}


	@Override
	@JsonProperty("type")
	public String repositoryType() {
		return instance.repositoryType();
	}



	@Override
	public String applicationType() {
		return instance.applicationType();
	}

	@Override
	public String toString() {
		return getRepositoryName()+": "+repositoryType()+"=>"+applicationType();
	}



	@Override
	public String getDeployment() {
		return instance.getDeployment();
	}


    @Override
    public void refreshApplicationLocking() throws IOException {
        instance.refreshApplicationLocking();
        
    }


	@Override
	public Set<String> getAllowedProfiles() {
		return instance.getAllowedProfiles();
	}


	@Override
	public File getTempFolder() {
		return instance.getTempFolder();
	}


	@Override
	public File getOutputFolder() {
		return instance.getOutputFolder();
	}


	@Override
	public Map<String, Object> getDeploymentSettings(Map<String, Object> source) {
		return instance.getDeploymentSettings(source);
	}


	public void setSessions(Map<String, TipiCallbackSession> session) {
		if(session!=null) {
			this.sessions = Collections.unmodifiableMap(session);
		} else {
			this.sessions = Collections.emptyMap();
		}
	}

	public Map<String,Map<String,String>> getSessions() {
		final Map<String,Map<String,String>> result = new HashMap<>();
		for (TipiCallbackSession ts : this.sessions.values()) {
			result.put(ts.getSessionId(), ts.toMap());
		}
		return result;
	}
}
