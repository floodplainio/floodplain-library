package com.dexels.navajo.repository.core.impl;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryInstance;


public abstract class RepositoryInstanceImpl implements RepositoryInstance {
    private final static Logger logger = LoggerFactory.getLogger(RepositoryInstanceImpl.class);

	protected String repositoryName;
	protected File applicationFolder;
	protected String deployment;
	
	private final Map<String,Object> settings = new HashMap<String, Object>();

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

	
	@Override
	public String getDeployment() {
		if(deployment!=null && !"".equals(deployment)) {
			return deployment;
		}
		String envDeployment = System.getProperty("DEPLOYMENT");
		return envDeployment;
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
