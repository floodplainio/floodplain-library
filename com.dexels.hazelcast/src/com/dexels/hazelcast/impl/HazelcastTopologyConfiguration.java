package com.dexels.hazelcast.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

@Component(name="dexels.hazelcast.topology",configurationPolicy=ConfigurationPolicy.OPTIONAL)
public class HazelcastTopologyConfiguration {
	
	
	private static final String DEFAULT_PATH = "/etc/navajocluster.xml";
	private static final String DEXELS_HAZELCAST_CONFIG_PID = "dexels.hazelcast.config";

	private ConfigurationAdmin configAdmin;
    private ServiceRegistration<ClassLoader> classLoaderService;

	private Configuration config;

	private final static Logger logger = LoggerFactory.getLogger(HazelcastTopologyConfiguration.class);

	@Activate
	public void activate(Map<String,Object> settings, BundleContext bundleContext) throws IOException {
		Map<String,String> env = System.getenv();
		String cluster = getenv(env, "CLUSTER");
		String instance = getenv(env, "INSTANCENAME");
		String host = getenv(env, "HOSTNAME");
		String clusterPort = getenv(env, "CLUSTER_PORT");
		
		Boolean useDefaultClassloader = false;
		String defaultClassloaderString = getenv(env, "CLUSTER_USE_DEFAULT_CLASSLOADER");
		if (defaultClassloaderString != null) {
		    useDefaultClassloader = Boolean.valueOf(defaultClassloaderString);
		}
		
		
		String path = null;
		if(settings!=null && settings.get("path")!=null) {
			path = (String) settings.get("path");
		}
		if(path==null) {
			path = getenv(env, "CLUSTER_PATH");
		}
		if(path==null) {
			path = DEFAULT_PATH;
		}
		File filePath = new File(path);
		
		if (useDefaultClassloader) {
            Dictionary<String,Object> classsettings = new Hashtable<String,Object>();
            classsettings.put("tag", "hazelcast");
            classLoaderService = bundleContext.registerService(ClassLoader.class, getClass().getClassLoader(), classsettings);
        }
		
		String localInterface = getenv(env, "HAZELCAST_INTERFACE");

		
		
		if(!filePath.exists() ||!filePath.isFile() || instance==null || host==null || cluster==null) {
			String simple = getenv(env, "HAZELCAST_SIMPLE");
			if(simple!=null) {
				logger.info("Configuring simple hazelcast topology: Cluster: {} instance: {} port: {}",cluster,instance,clusterPort);
				configureSimple(cluster, instance,clusterPort,localInterface);
			} else {
				logger.warn("No complete topology config found for navajocluster: {} : {} : {} : {}. Not continuing",path,instance,host,cluster);
			}

		} else {
			configure(cluster, instance, host, clusterPort, filePath);
		}
		
		
          
	}


	@Deactivate
    public void deactivate() {
        if (config != null) {
            try {
                config.delete();
            } catch (IOException e) {
                logger.error("Error:", e);
            }
        }
        if (classLoaderService != null) {
            classLoaderService.unregister();
        }

    }

	private void configureSimple(String cluster, String instance,String clusterPortString, String localInterface) {
		try {
			config = configAdmin.getConfiguration(DEXELS_HAZELCAST_CONFIG_PID);
			Dictionary<String, Object> settings = new Hashtable<>();
			if(cluster!=null) {
				if(clusterPortString!=null) {
					Integer port = Integer.parseInt(clusterPortString);
					settings.put("cluster.port",port);
				}
				String uid = UUID.randomUUID().toString();
				cluster += "-" + uid.substring(0, uid.indexOf('-'));
				settings.put("cluster.name",cluster);
				
				if(instance!=null) {
					settings.put("cluster.instance",instance);
				}
				if(localInterface!=null) {
					settings.put("cluster.interface",localInterface);
				}
			}
			config.update(settings);
		} catch (Exception e) {
			logger.error("Error: ", e);
		}
		
	}

	private void configure(String clusterName, String instanceName, String hostName,String clusterPortString, File topologyFile) throws IOException {
		int clusterPort = 5701;
		if (clusterPortString!=null) {
			clusterPort = Integer.parseInt(clusterPortString);
		}
		try {
			config = configAdmin.getConfiguration(DEXELS_HAZELCAST_CONFIG_PID);
			Dictionary<String, Object> settings = createSettings(clusterName,
					instanceName, topologyFile,null, clusterPort,hostName);
			config.update(settings);
		} catch (Exception e) {
			logger.error("Error: ", e);
		}
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

	protected Document loadDocument(InputStream docStream) throws IOException {
		try {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
			dBuilder = dbFactory.newDocumentBuilder();
			return dBuilder.parse(docStream);
		} catch (ParserConfigurationException e1) {
			throw new IOException("Parser problem. ",e1);
		} catch (SAXException e1) {
			throw new IOException("Parser problem. ",e1);
		}

	}
	
	private List<String> getClusterMembers(Document doc, String cluster, Dictionary<String, Object> settings,String instanceName,String hostName) throws IOException {
		final List<String> members = new ArrayList<>();
		NodeList list = doc.getDocumentElement()
				.getElementsByTagName("cluster");
		String ip = null;
		int port = -1;
		for (int temp = 0; temp < list.getLength(); temp++) {
			Node nNode = list.item(temp);
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				Element e = (Element) nNode;
				
				NodeList nl = e.getElementsByTagName("member");
				if (cluster.equals(e.getAttribute("name"))) {
					for (int i = 0; i < nl.getLength(); i++) {
						Element member = (Element) nl.item(i);
						String servername = member.getAttribute("servername");
						String memberport = member.getAttribute("port");
						String memberip = member.getAttribute("ip");
	
						// it's me!
						if(hostName.equals(servername) && instanceName.equals(member.getAttribute("instancename")) ) {
							ip = member.getAttribute("ip");
							String portString = member.getAttribute("port");
							if(portString!=null) {
								port = Integer.parseInt(portString);
							}
						} else {
							// only when not me
							members.add(memberip+":"+memberport);
						}
					}
				}
			}
		}
		if(ip!=null) {
			settings.put("cluster.hostName", ip);
			logger.info("using hostName: {}", ip);
		} else {
			logger.warn("No ip found, so no cluster.hostName. This could be a problem");
		}
		if(port!=-1) {
			settings.put("cluster.externalPort", port);
			logger.info("using externalPort: {}", port);
		} else {
			logger.warn("No port found, so no cluster.hostName. This could be a problem");

		}

		return members;
	}
	
	public Dictionary<String, Object> createSettings(String clusterName,
			String instanceName, File topologyFile, InputStream topologyStream, int clusterPort, String hostName)
			throws IOException {
		Dictionary<String, Object> settings;
		try {
			settings = new Hashtable<>();
			settings.put("cluster.name", clusterName);
			settings.put("cluster.instance",  instanceName + "@" + hostName);
			settings.put("cluster.port", clusterPort);

			if(topologyStream==null) {
				topologyStream = new FileInputStream(topologyFile);
			}
			logger.info("Creating topology for cluster: {} instance: {} port: {} hostName: {}",clusterName,instanceName,clusterPort,hostName);
			List<String> list = getClusterMembers(loadDocument(topologyStream), clusterName,settings,instanceName,hostName);
			String[] ss = new String[list.size()];
			int index = 0;
			for (String e : list) {
				ss[index++] = e;
			}
			settings.put("cluster.members", ss);
		} finally {
			if(topologyStream!=null) {

				topologyStream.close();
			}
		}
		return settings;
	}
	
	
	private String getenv(Map<String, String> env, String key) {
		String result = env.get(key);
		if(result!=null) {
			return result;
		}
		return System.getProperty(key);
	}

}
