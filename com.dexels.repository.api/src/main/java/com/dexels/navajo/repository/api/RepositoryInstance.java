package com.dexels.navajo.repository.api;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RepositoryInstance {
	
	public File getRepositoryFolder();
	
	public File getTempFolder();

	public File getOutputFolder();

	public String getRepositoryName();

	public Map<String, Object> getSettings();

	public String getDeployment();
	
	public Set<String> getAllowedProfiles();
	
	public Map<String,Object> getDeploymentSettings(Map<String,Object> source);

}