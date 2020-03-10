package com.dexels.navajo.repository.file.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.core.impl.RepositoryInstanceImpl;

public class FileRepositoryInstanceImpl extends RepositoryInstanceImpl implements RepositoryInstance {

	String configuredRepositoryFolder;

	String configuredRepositoryType;

	String configuredRepositoryName;
	
	String configuredRepositoryDeployment;
	
	public FileRepositoryInstanceImpl() {
	}
	
	private final static Logger logger = LoggerFactory
			.getLogger(FileRepositoryInstanceImpl.class);

	public String configuredRepositoryFolder() {
		return configuredRepositoryFolder;
	}


	public String configuredRepositoryType() {
		return configuredRepositoryType;
	}


	public String configuredRepositoryName() {
		return configuredRepositoryName;
	}


	public String configuredRepositoryDeployment() {
		return configuredRepositoryDeployment;
	}

	public void activate() throws IOException {
		logger.info("Activating repository with folder: {}",configuredRepositoryFolder);
		String path = configuredRepositoryFolder();
		type = configuredRepositoryType();
		repositoryName = configuredRepositoryName();
		deployment = configuredRepositoryDeployment();
		applicationFolder = findConfiguration(path);

	}
	public void activate(Map<String,Object> configuration) throws IOException {
		try {
			String path = (String) configuration.get("repository.folder");
			type = (String) configuration.get("repository.type");
			repositoryName = (String) configuration.get("repository.name");
			deployment = (String) configuration.get("repository.deployment");
			getSettings().putAll(configuration);
			applicationFolder = findConfiguration(path);
		} catch (Throwable e) {
			logger.error("Activation error: ",e);
			throw e;
		}
	}
	
	
	private File findConfiguration(String path)
			throws IOException {
		
		if(path==null || "".equals(path)) {
			throw new IllegalArgumentException("Missing path in filerepo ");
		}
		File storeFolder = null;
		File suppliedPath = new File(path);
		logger.info("Repository Manager is using path: "+suppliedPath.getAbsolutePath());
		if(suppliedPath.isAbsolute()) {
			storeFolder = suppliedPath;
		} else {
			throw new IllegalArgumentException("Path should be absolute: "+suppliedPath);
		}

		if(!storeFolder.exists()) {
			storeFolder.mkdirs();
		}
		return storeFolder;
	}

	public Map<String,Object> getSettings() {
		return super.getSettings();
	}
}
