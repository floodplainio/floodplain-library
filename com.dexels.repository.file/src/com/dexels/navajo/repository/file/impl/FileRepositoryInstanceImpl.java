package com.dexels.navajo.repository.file.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;

@Component(configurationPolicy=ConfigurationPolicy.REQUIRE, name="dexels.repository.file",immediate=true)
public class FileRepositoryInstanceImpl extends BaseFileRepositoryInstanceImpl implements RepositoryInstance {

	public FileRepositoryInstanceImpl() {
	}
	
	private final static Logger logger = LoggerFactory
			.getLogger(FileRepositoryInstanceImpl.class);

	@Activate
	public void activate(Map<String,Object> configuration) throws IOException {
		try {
			String path = (String) configuration.get("repository.folder");
			type = (String) configuration.get("repository.type");
			repositoryName = (String) configuration.get("repository.name");
			deployment = (String) configuration.get("repository.deployment");
			final String fileInstallPath= (String) configuration.get("felix.fileinstall.filename");
			getSettings().putAll(configuration);
			applicationFolder = findConfiguration(path,fileInstallPath);
			super.setupMonitoredFolders(parseLocations((String)configuration.get("monitored")));
			registerFileInstallLocations(parseLocations((String)configuration.get("fileinstall")));
		} catch (Throwable e) {
			logger.error("Activation error: ",e);
			throw e;
		}
	}
	
	
	
	private List<String> parseLocations(String locations) {
		if(locations==null || "".equals(locations)) {
			return Collections.emptyList();
		}
		return Arrays.asList(locations.split(","));
	}

	@Deactivate
	public void deactivate() {

		if(watchDir!=null) {
			try {
				watchDir.close();
			} catch (IOException e) {
				logger.error("Error: ", e);
			}
			watchDir = null;
		}
		super.deregisterFileInstallLocations();

	}
	
	public Map<String,Object> getSettings() {
		return super.getSettings();
	}

	@Override
	public String repositoryType() {
		return "file";
	}

	@Override
	@Reference(cardinality=ReferenceCardinality.OPTIONAL,policy=ReferencePolicy.DYNAMIC,unbind="clearEventAdmin")
	public void setEventAdmin(EventAdmin eventAdmin) {
		super.setEventAdmin(eventAdmin);
	}
	
	

	@Override
	public void clearEventAdmin(EventAdmin eventAdmin) {
		super.clearEventAdmin(eventAdmin);
	}
	
	@Reference(cardinality=ReferenceCardinality.MULTIPLE,unbind="removeOperation",policy=ReferencePolicy.DYNAMIC)
	@Override
	public void addOperation(AppStoreOperation op, Map<String,Object> settings) {
		super.addOperation(op, settings);
	}

	@Override
	public void removeOperation(AppStoreOperation op,
			Map<String, Object> settings) {
		super.removeOperation(op, settings);
		
	}



	@Reference(cardinality=ReferenceCardinality.MANDATORY,unbind="clearConfigAdmin",policy=ReferencePolicy.DYNAMIC)
	@Override
	public void setConfigAdmin(ConfigurationAdmin configAdmin) {
		super.setConfigAdmin(configAdmin);
	}

	@Override
	public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
		super.clearConfigAdmin(configAdmin);
	}

    @Override
    public void refreshApplicationLocking() throws IOException {
        refreshApplication();
        return;   
    }

}
