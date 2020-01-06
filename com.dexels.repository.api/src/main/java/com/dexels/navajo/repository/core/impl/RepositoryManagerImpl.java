package com.dexels.navajo.repository.core.impl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryManager;

@Component(name="navajo.repository.manager",configurationPolicy=ConfigurationPolicy.REQUIRE,immediate=true)
public class RepositoryManagerImpl implements RepositoryManager {
	protected String organization;
	private File repositoryFolder;
	private File sshFolder;
	private File configurationFolder;
	
	private final static Logger logger = LoggerFactory
			.getLogger(RepositoryManagerImpl.class);
	private File storeFolder;
	private File tempFolder;
	private File outputFolder;
	

	@Activate
	public void activate(Map<String,Object> configuration) throws IOException {

		String path = (String) configuration.get("storage.path");
		String tempPath = (String) configuration.get("storage.temp");
		String outputPath = (String) configuration.get("storage.output");
		final String fileInstallPath= (String) configuration.get("felix.fileinstall.filename");
		logger.info("Activating repository manager with settings: {}",configuration);
		storeFolder = findConfiguration(path,fileInstallPath);

		repositoryFolder = new File(storeFolder,"repositories");
		if(!repositoryFolder.exists()) {
			repositoryFolder.mkdirs();
		}
		if(tempPath!=null) {
			this.tempFolder = new File(tempPath);
		} else {
			this.tempFolder = new File(storeFolder,"temp");
		}
		if(!tempFolder.exists()) {
			tempFolder.mkdirs();
		}
		if(outputPath!=null) {
			this.outputFolder = new File(outputPath);
		} else {
			this.outputFolder = new File(storeFolder,"output");
		}
		if(!outputFolder.exists()) {
			outputFolder.mkdirs();
		}

		sshFolder = new File(storeFolder,"gitssh");
		if(!sshFolder.exists()) {
			sshFolder.mkdirs();
		}
		configurationFolder = new File(storeFolder,"etc");
		if(!configurationFolder.exists()) {
			configurationFolder.mkdirs();
		}
		logger.info("Repository manager activated. Using repositoryfolder: "+repositoryFolder.getAbsolutePath());

	}

	private File findConfiguration(String path, String fileInstallPath)
			throws IOException {
		
		if(path==null || "".equals(path)) {
			path = System.getProperty("storage.path");
		}
		File storeFolder = null;
		if(path==null) {
			logger.info("No storage.path found, now trying to retrieve from felix.fileinstall.filename");
			storeFolder = findByFileInstaller(fileInstallPath,"storage");
		} else {
			
			File suppliedPath = new File(path);
			logger.info("Repository Manager is using path: "+suppliedPath.getAbsolutePath());
			if(suppliedPath.isAbsolute()) {
				storeFolder = suppliedPath;
			} else {
				storeFolder =findByFileInstaller(fileInstallPath,path);
			}
		}
		if(storeFolder==null ) {
			storeFolder = findByFileInstaller(fileInstallPath,"storage");
		}
		if(storeFolder==null) {
			throw new IOException("No storage.path set in configuration!");
		}
		if(!storeFolder.exists()) {
			storeFolder.mkdirs();
		}
		return storeFolder;
	}


	private File findByFileInstaller(final String fileNamePath, String storagePath) {
		if(fileNamePath==null) {
			return null;
		}
		try {
			URL url = new URL(fileNamePath);
			File f;
			try {
			  f = new File(url.toURI());
			} catch(URISyntaxException e) {
			  logger.trace("bad url ",e);
			  f = new File(url.getPath());
			}
			if(f!=null) {
				File etc = f.getParentFile();
				if(etc!=null) {
					File root = etc.getParentFile();
					if(root!=null) {
						File storage = new File(root,storagePath);
						if(!storage.exists()) {
							storage.mkdirs();
						}
						return storage;
					}
				}
			}
		} catch (MalformedURLException e) {
			logger.warn("Fileinstall.filename based resolution also failed.",e);
		}
		return null;
	}
	
	@Deactivate
	public void deactivate() {

	}


	@Override
	public File getSshFolder() {
		return sshFolder;
	}

	@Override
	public File getRepositoryFolder() {
		return repositoryFolder;
	}

	@Override
	public File getConfigurationFolder() {
		return configurationFolder;
	}

	@Override
	public File getOutputFolder() {
		return outputFolder;
	}

	@Override
	public File getTempFolder() {
		return tempFolder;
	}


}
