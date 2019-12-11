package com.dexels.navajo.tipi.dev.server.appmanager.impl;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.osgi.framework.BundleContext;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name="tipi.dev.store.system",immediate=true)
public class SystemAppStoreConfigurator {

	private ConfigurationAdmin configAdmin;
	
	private final static Logger logger = LoggerFactory
			.getLogger(SystemAppStoreConfigurator.class);

	private Configuration configuration;

	@Activate
	public void activate(BundleContext bc) throws IOException {
		loadConfig();
	}

	private void loadConfig() throws IOException {
		String authorize = System.getProperty("TIPI_STORE_AUTHORIZE");
		String organization = System.getProperty("TIPI_STORE_ORGANIZATION");
		String applicationname = System.getProperty("TIPI_STORE_APPLICATIONNAME");
		String manifestCodebase = System.getProperty("TIPI_STORE_MANIFESTCODEBASE");
		String codebase = System.getProperty("TIPI_STORE_CODEBASE");
		String clientid = System.getProperty("TIPI_STORE_CLIENTID");
		String clientsecret = System.getProperty("TIPI_STORE_CLIENTSECRET");

		if(authorize==null) {
			logger.warn("No 'TIPI_STORE_AUTHORIZE' set, so no appstore configurations will be injected.");
			return;
		}
		injectConfig(authorize,organization,applicationname,manifestCodebase,codebase,clientid,clientsecret);
	}
	
	@Reference(unbind="clearConfigAdmin",policy=ReferencePolicy.DYNAMIC)
	public void setConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = configAdmin;
	}

	/**
	 * @param configAdmin the configAdmin to remove 
	 */
	public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = null;
	}

	private void injectConfig(String authorize, String organization,
			String applicationname, String manifestCodebase, String codebase,
			String clientid, String clientsecret) throws IOException {

//		Configuration c = createOrReuse("tipi.dev.appstore.manager", "(tipi.store.name=system.tipi.store)");
		configuration = configAdmin.getConfiguration("tipi.dev.appstore.manager",null);

		Dictionary<String,Object> properties = new Hashtable<String,Object>();
		if(clientid!=null) {
			properties.put("tipi.store.clientid", clientid);
		}
		properties.put("tipi.store.name", "system.tipi.store");

		if(clientsecret!=null) {
			properties.put("tipi.store.clientsecret", clientsecret);
		}
		if(organization!=null) {
			properties.put("tipi.store.organization", organization);
		}
		if(applicationname!=null) {
			properties.put("tipi.store.applicationname", applicationname);
		} else {
			properties.put("tipi.store.applicationname", "default");
			
		}
		if(codebase!=null) {
			properties.put("tipi.store.codebase", codebase);
		} else {
			properties.put("tipi.store.codebase", "http://localhost");
		}
		if(manifestCodebase!=null) {
			properties.put("tipi.store.manifestcodebase", manifestCodebase);
		} else {
			properties.put("tipi.store.manifestcodebase", "http://localhost");
		}
		if(authorize!=null) {
			properties.put("authorize", authorize);
		} else {
			properties.put("authorize", "false");

		}
		configuration.update(properties);	
	}
}
