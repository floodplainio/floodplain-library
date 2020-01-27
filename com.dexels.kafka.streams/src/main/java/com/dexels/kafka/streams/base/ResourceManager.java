package com.dexels.kafka.streams.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Dictionary;
import java.util.Hashtable;

import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.navajo.repository.api.RepositoryInstance;


@Component(name="kafka.stream.resourcemanager", service = {ResourceManager.class}, property = {"event.topics=repository/change"}, immediate=true)
public class ResourceManager {
	
	private static final String KAFKA_PUBLISHER_PID = "navajo.resource.kafkatopicpublisher";
	private static final String KAFKA_SUBSCRIBER_PID = "navajo.resource.kafkatopicsubscriber";


	private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);
	private RepositoryInstance repositoryInstance;
	private StreamConfiguration configuration;
	private ConfigurationAdmin configAdmin;


	
	@Activate
	public void activate() throws IOException {
		File resources = new File(this.repositoryInstance.getRepositoryFolder(),"config/resources.xml");
		try (InputStream r = new FileInputStream(resources)){
			this.configuration = StreamConfiguration.parseConfig(this.repositoryInstance.getDeployment(),r);
			createPublisherConfig(createOrReuse(KAFKA_PUBLISHER_PID, "(source=kafkastreams)"));
			createSubscriberConfig(createOrReuse(KAFKA_SUBSCRIBER_PID, "(source=kafkastreams)"));
		}
	}
	
	private void createPublisherConfig(Configuration cc) throws IOException {
		 Dictionary<String, Object> configattribute = new Hashtable<String, Object>();
		 configattribute.put("hosts", this.configuration.kafkaHosts());
		 configattribute.put("retries", "10");
		 updateIfChanged(cc,configattribute);
	}

	private void createSubscriberConfig(Configuration cc) throws IOException {
		 Dictionary<String, Object> configattribute = new Hashtable<String, Object>();
		 configattribute.put("hosts", this.configuration.kafkaHosts());
		 configattribute.put("max", "100");
		 configattribute.put("wait", "10000");
		 updateIfChanged(cc,configattribute);
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearRepositoryInstance")
	public void setRepositoryInstance(RepositoryInstance instance) {
		this.repositoryInstance = instance;
	}

	public void clearRepositoryInstance(RepositoryInstance instance) {
		this.repositoryInstance = null;
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



	private Configuration createOrReuse(String pid, final String filter)
			throws IOException {
		Configuration cc = null;
		try {
			Configuration[] c = configAdmin.listConfigurations(filter);
			if(c!=null && c.length>1) {
				logger.warn("Multiple configurations found for filter: {}", filter);
			}
			if(c!=null && c.length>0) {
				cc = c[0];
			}
		} catch (InvalidSyntaxException e) {
			logger.error("Error in filter: {}",filter,e);
		}
		if(cc==null) {
			cc = configAdmin.createFactoryConfiguration(pid,null);
		}
		return cc;
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

}
