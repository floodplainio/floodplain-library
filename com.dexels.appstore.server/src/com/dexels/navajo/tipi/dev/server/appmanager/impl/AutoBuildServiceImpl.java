package com.dexels.navajo.tipi.dev.server.appmanager.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreData;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.impl.JnlpBuild;

@Component(immediate=true,name="tipi.dev.appstore.autobuild",service={})
public class AutoBuildServiceImpl implements Runnable {

	
	private final static Logger logger = LoggerFactory.getLogger(AutoBuildServiceImpl.class);
	
	private JnlpBuild jnlpBuild;
	private AppStoreData appStoreData;
	private final ExecutorService executorService = Executors.newFixedThreadPool(1);
	protected final Map<String,RepositoryInstance> applications = new HashMap<String, RepositoryInstance>();
	private boolean active = false;
	
	@Activate
	public void activate(Map<String,Object> settings) {
//		Map<String, Map<String, ?>> data = appStoreData.getApplicationData();
		this.active = true;
		logger.info("Starting thread");
		executorService.execute(this);
	}
	
	@Deactivate
	public void deactivate() {
		this.active = false;
		executorService.shutdownNow();
	}
	
	@Reference(unbind="clearJnlpBuild",policy=ReferencePolicy.DYNAMIC,target="(name=build)")
	public void setJnlpBuild(JnlpBuild jnlpBuild) {
		this.jnlpBuild = jnlpBuild;
	}

	public void clearJnlpBuild(AppStoreOperation jnlpBuild) {
		this.jnlpBuild = null;
	}
	
	@Reference(unbind="clearAppStoreData",policy=ReferencePolicy.DYNAMIC)
	public void setAppStoreData(AppStoreData appStoreData) {
		this.appStoreData = appStoreData;
		logger.info("Appstoredata bound");
	}

	public void clearAppStoreData(AppStoreData appStoreData) {
		this.appStoreData = null;
	}


	@Reference(unbind="removeRepositoryInstance",policy=ReferencePolicy.DYNAMIC,cardinality=ReferenceCardinality.MULTIPLE)
	public void addRepositoryInstance(RepositoryInstance a) {
		applications.put(a.getRepositoryName(), a);
	}
	
	public void removeRepositoryInstance(RepositoryInstance a) {
		applications.remove(a.getRepositoryName());
	}

	@Override
	public void run() {
		while(active) {
			try {
				Map<String, Map<String, ?>> data = appStoreData.getApplicationData();
				Map<String, ?> d = data.get("applications");
				
				for (Entry<String, ?> e : d.entrySet()) {
					RepositoryInstanceWrapper ri = (RepositoryInstanceWrapper) e.getValue();

					if(!ri.isBuilt()) {
                       if(jnlpBuild!=null) {
                           jnlpBuild.build(ri);
                       }
                   }
				}
			} catch (Throwable e) {
				logger.error("Error in autobuild. Continuing. ", e);
			} finally {
				try {
					// TODO Put in config file
					Thread.sleep(120000);
				} catch (InterruptedException e) {
				}
				logger.info("Iteration complete");
			}
		}
	}

}
