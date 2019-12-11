package com.dexels.monitor.rackermon.discovery;

import java.io.IOException;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.kubernetes.Cluster;
import com.dexels.hazelcast.kubernetes.ClusterMember;
import com.dexels.hazelcast.kubernetes.Deployment;
import com.dexels.hazelcast.kubernetes.DiscoverClusterMembers;
import com.dexels.hazelcast.kubernetes.ReplicaSet;
import com.dexels.hazelcast.kubernetes.StatefulSet;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;

@Component(name="dexels.rackermon.discovery.kubernetes")
public class KubernetesDiscovery {

	
	private final static Logger logger = LoggerFactory.getLogger(KubernetesDiscovery.class);
	private ConfigurationAdmin configAdmin;

	private DiscoverClusterMembers discover;
	private Disposable activateFlow;

	@Activate
	public void activate() {
		this.activateFlow = Flowable.interval(50, TimeUnit.SECONDS)
			.subscribe(e->discover());
	}

	
	@Deactivate
	public void deactivate() {
		if(activateFlow!=null) {
			activateFlow.dispose();
			activateFlow = null;
		}
	}
	private void discover() {
		try {
			logger.info("Activating dexels.rackermon.discovery.kubernetes");
			discover.listNamespaces().forEach(ns->{
				
			});
			discover.listNamespaces().stream().forEach(namespace ->{
				injectNamespace(namespace);
				logger.info("Activating namespace: {}",namespace);
				injectNamespaceElements(namespace);
				List<ClusterMember> members = discover.listPods(namespace);
                members.stream().filter(e -> e.labels() != null)
					.filter(e->e.labels().get("rackermon")!=null)
					.forEach(m->{
						try {
							System.err.println("MEMBERLABELS: "+m.labels());
							injectMember(m);
						} catch (Exception e1) {
							logger.error("Error injecting: ", e1);
						}
					});
                deleteNotFoundMembers(members, namespace);
			});
		} catch (Throwable e) {
			logger.error("General discovery error: ", e);
		}
	}
	
    private void injectNamespaceElements(String namespace) {
//		discover.listServices(namespace).stream()
//			.filter(serv -> serv.labels() != null)
//			.filter(serv -> serv.labels().get("rackermon") != null )
//			.forEach(service ->{
//				injectClusterService(service,namespace);
//			});

        discover.listDeployments(namespace).stream()
	        .filter(dep -> dep.templateLabels() != null)
	        .filter(dep -> dep.templateLabels().get("rackermon") != null)
	        .forEach(deployment -> {
	        	System.err.println("Injecting deployment: "+deployment.name());
	            injectClusterDeployment(deployment, namespace);
	        });
        
        discover.listStatefulSets(namespace).stream()
        .filter(dep -> dep.templateLabels() != null)
        .filter(dep -> dep.templateLabels().get("rackermon") != null)
        .forEach(statefulSet -> {
        	System.err.println("Injecting deployment: "+statefulSet.name());
        	injectClusterStatefulSet(statefulSet, namespace);
        });
        
        discover.listReplicaSets(namespace).forEach(replicaSet->{
        	injectReplicaSetSet(replicaSet, namespace);
        });
	}
    
	private void injectReplicaSetSet(ReplicaSet replicaSet, String namespace) {
		Configuration cc;
		try {
            cc = createOrReuse("dexels.monitor.replicaset", "(name=" + replicaSet.name()+")");
            Hashtable<String, Object> settings = new Hashtable<String, Object>();
            final Dictionary<String, Object> properties = cc.getProperties();
            if (properties != null) {
                Enumeration<String> en = properties.keys();
                while (en.hasMoreElements()) {
                    final String key = en.nextElement();
                    settings.put(key, properties.get(key));
                }
            }
            settings.put("name", replicaSet.name());
            settings.put("group", namespace);
            replicaSet.parent().ifPresent(parent->settings.put("parent", parent));
            settings.putAll(replicaSet.labels());
			updateIfChanged(cc, settings);
		} catch (IOException e) {
			logger.error("Error: ", 1);
		}		
	}


	private void injectNamespace(String namespace) {
		Configuration cc;
		try {
			cc = createOrReuse("dexels.monitor.group", "(name="+namespace+")");
			Hashtable<String,Object> settings = new Hashtable<>();
			final Dictionary<String, Object> properties = cc.getProperties();
			if(properties!=null) {
				Enumeration<String> en = properties.keys();
				while (en.hasMoreElements()) {
					final String nextElement = en.nextElement();
					settings.put(nextElement,properties.get(nextElement));
				}
			}
			settings.put("name", namespace);
			settings.put("priority", "0");
			updateIfChanged(cc, settings);
		} catch (IOException e) {
			logger.error("Error: ", 1);
		}
	}


    private void injectClusterDeployment(Deployment deployment, String namespace) {
        Configuration cc;
        try {
            cc = createOrReuse("dexels.monitor.deployment", "(name=" + deployment.name()+")");
            Hashtable<String, Object> settings = new Hashtable<String, Object>();
            final Dictionary<String, Object> properties = cc.getProperties();
            if (properties != null) {
                Enumeration<String> en = properties.keys();
                while (en.hasMoreElements()) {
                    final String key = en.nextElement();
                    settings.put(key, properties.get(key));
                }
            }
            settings.put("name", deployment.name());
            settings.put("group", namespace);
            settings.put("type", deployment.templateLabels().get("rackermon"));
            settings.put("cluster", deployment.labels().get("release"));

            for (Entry<String, String> elt : deployment.labels().entrySet()) {
                settings.put(elt.getKey(), elt.getValue());
            }
            for (Entry<String, String> elt : deployment.status().entrySet()) {
                settings.put(elt.getKey(), elt.getValue());
            }
            for (Entry<String, String> elt : deployment.templateLabels().entrySet()) {
                settings.put(elt.getKey(), elt.getValue());
            }
            for (Entry<String, String> elt : deployment.eventsMap().entrySet()) {
                settings.put(elt.getKey(), elt.getValue());
            }
            settings.putAll(deployment.labels());
            settings.putAll(deployment.status());
            settings.putAll(deployment.templateLabels());
            updateIfChanged(cc, settings);
        } catch (IOException e) {
            logger.error("Error: ", e);
        }
        
    }
    private void injectClusterStatefulSet(StatefulSet deployment, String namespace) {
        Configuration cc;
        try {
            cc = createOrReuse("dexels.monitor.deployment", "(name=" + deployment.name() + "-dp)");
            Hashtable<String, Object> settings = new Hashtable<>();
            final Dictionary<String, Object> properties = cc.getProperties();
            if (properties != null) {
                Enumeration<String> en = properties.keys();
                while (en.hasMoreElements()) {
                    final String key = en.nextElement();
                    settings.put(key, properties.get(key));
                }
            }
            settings.put("name", deployment.name());
            settings.put("group", namespace);
            settings.put("type", deployment.templateLabels().get("rackermon"));
            settings.put("cluster", deployment.labels().get("release"));

            for (Entry<String, String> elt : deployment.labels().entrySet()) {
                settings.put(elt.getKey(), elt.getValue());
            }
            for (Entry<String, String> elt : deployment.status().entrySet()) {
                settings.put(elt.getKey(), elt.getValue());
            }
            for (Entry<String, String> elt : deployment.templateLabels().entrySet()) {
                settings.put(elt.getKey(), elt.getValue());
            }
            for (Entry<String, String> elt : deployment.eventsMap().entrySet()) {
                settings.put(elt.getKey(), elt.getValue());
            }
            settings.putAll(deployment.labels());
            settings.putAll(deployment.status());
            settings.putAll(deployment.templateLabels());
            updateIfChanged(cc, settings);
        } catch (IOException e) {
            logger.error("Error: ", e);
        }
        
    }


    private void injectClusterService(Cluster service, String namespace) {
		Configuration cc;
		try {
			
            cc = createOrReuse("dexels.monitor.cluster", "(name=" + service.labels().get("release") + ")");
            Hashtable<String,Object> settings = new Hashtable<>();
			final Dictionary<String, Object> properties = cc.getProperties();
			if(properties!=null) {
				Enumeration<String> en = properties.keys();
				while (en.hasMoreElements()) {
					final String key = en.nextElement();
					settings.put(key,properties.get(key));
				}
			}
			settings.put("name", service.labels().get("release"));
			settings.put("group", namespace);
			settings.put("type", service.labels().get("rackermon"));
			for (Entry<String,String> elt : service.labels().entrySet()) {
				settings.put(elt.getKey(), elt.getValue());
			}
			settings.putAll(service.labels());
			updateIfChanged(cc, settings);
		} catch (IOException e) {
			logger.error("Error injecting: ", 1);
		}
	}


	private void injectMember(ClusterMember m) throws IOException {
		String type = m.labels().get("rackermon");
		if(!m.ip().isPresent()) {
			logger.warn("Missing server ip for name: {}",m.name());
			return;
		}
		Configuration cc = createOrReuse("dexels.monitor.server", "(name="+m.name()+")");
		Hashtable<String,Object> settings = new Hashtable<>();
		final Dictionary<String, Object> properties = cc.getProperties();
		if(properties!=null) {
			Enumeration<String> en = properties.keys();
			while (en.hasMoreElements()) {
				final String key = en.nextElement();
				settings.put(key,properties.get(key));
			}
		}
		Integer defaultPort = m.ports().get(0);
		Optional<String> memberIp = m.ip();
		settings.put("name", m.name());
		settings.put("containerName", m.name());
		settings.put("namespace", m.namespace());
		settings.put("serverName", m.name());
		m.parent().ifPresent(e->settings.put("parent",e));
		settings.put("status", m.statusPhase());
		m.statusMessage().ifPresent(msg->settings.put("statusMessage",msg));
		memberIp.ifPresent(ip->settings.put("ip", ip));
		settings.put("type", type);
		settings.put("ports", m.ports().stream().toArray(Integer[]::new));
		settings.put("defaultPort", defaultPort);
		settings.put("cluster", m.labels().get("release"));
		updateIfChanged(cc, settings);
		
	}
	
    private boolean existsInMembers(String namespace, List<ClusterMember> members, String podName) {
        for (ClusterMember member : members) {
            if (member.name().equals(podName) && member.namespace().equals(namespace)) {
                return true;
            }
        }
        return false;
    }

    private void deleteNotFoundMembers(List<ClusterMember> members, String namespace) {
        try {
            Configuration[] test = configAdmin.listConfigurations("(name=*)");
            Arrays.stream(test).filter(configuration -> configuration.getPid().contains("server"))
                    .filter(configuration -> ((String) configuration.getProperties().get("namespace")).equals(namespace))
                    .filter(configuration -> !existsInMembers(namespace, members,
                            (String) configuration.getProperties().get("containerName")))
                    .forEach(configuration -> {
                        try {
                            logger.info("Deleting configuration {} for non existing cluster",configuration.getPid());
                            configAdmin.getConfiguration(configuration.getPid()).delete();
                        } catch (IOException e) {
                            logger.error("Something went wrong while deleting innactive configuration",e);
                        }
                    });
        } catch (IOException e) {
            logger.error("IOException while deleting innactive configuration",e);
        } catch (InvalidSyntaxException e) {
            logger.error("InvalidSyntaxException while deleting innactive configuration",e);
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
	
	
	private Configuration createOrReuse(String pid, final String filter)
			throws IOException {
		Configuration cc = null;
		// mostly to prevent messy shutdowns
		if(configAdmin==null) {
			return null;
		}
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

    
	@Reference
	public void setDiscoverClusterMembers(DiscoverClusterMembers dc) {
		this.discover = dc;
	}
}
