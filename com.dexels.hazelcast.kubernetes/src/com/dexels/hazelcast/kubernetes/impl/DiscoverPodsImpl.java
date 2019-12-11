package com.dexels.hazelcast.kubernetes.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.kubernetes.Cluster;
import com.dexels.hazelcast.kubernetes.ClusterMember;
import com.dexels.hazelcast.kubernetes.Deployment;
import com.dexels.hazelcast.kubernetes.DiscoverClusterMembers;
import com.dexels.hazelcast.kubernetes.DiscoveryException;
import com.dexels.hazelcast.kubernetes.Event;
import com.dexels.hazelcast.kubernetes.ReplicaSet;
import com.dexels.hazelcast.kubernetes.StatefulSet;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.models.V1NamespaceList;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.util.Config;

@Component
public class DiscoverPodsImpl implements DiscoverClusterMembers {
	private CoreV1Api api;
	private String namespace;
    private AppsV1Api appsApi;
	
	private final static Logger logger = LoggerFactory.getLogger(DiscoverPodsImpl.class);


	@Activate
	public void activate() throws IOException {
		ApiClient client = createAppClient();
		Configuration.setDefaultApiClient(client);
		api = new CoreV1Api();
        appsApi = new AppsV1Api();
	}



	private ApiClient createFallbackAppClient() throws IOException {
		this.namespace = null;

        String confingEnv = System.getenv("KUBECONFIG");
        if (confingEnv == null || confingEnv.isEmpty()) {
            return Config.defaultClient();
        } else {
            return Config.fromConfig(confingEnv);
        }

	}

	private ApiClient createAppClient() throws IOException {
		String token;
		try {
			token = fileContent("/var/run/secrets/kubernetes.io/serviceaccount/token");
			this.namespace = fileContent("/var/run/secrets/kubernetes.io/serviceaccount/namespace");
//			String cert = fileContent("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
		} catch (IOException e) {
			logger.info("Detected that I'm not running inside kube, using fallback.");
			return createFallbackAppClient();
		}
		
		String host = System.getenv("KUBERNETES_SERVICE_HOST");
		String port = System.getenv("KUBERNETES_SERVICE_PORT");
		if(host==null || port == null) {
			logger.info("Detected that I'm not running inside kube: no KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT detected, using fallback.");
			return createFallbackAppClient();
		}
		String url = "https://"+host+":"+port;
		ApiClient client = Config.fromToken(url, token,false);
		return client;
	}
	
	public String fileContent(String path) throws IOException {
		List<String> l = Files.readAllLines(Paths.get(path));
		return l.stream().collect( Collectors.joining( "" ));
	}
	
	
	@Override
	public List<Cluster> listServices(String namespace) {
		try {
			return api.listNamespacedService(namespace, null, null, null, null, null, null, null, null).getItems()
					.stream()
					.map(srv->{
						String name = srv.getMetadata().getName();
						Map<String,String> labels = srv.getMetadata().getLabels();
						return new ClusterImpl(name,labels);
					})
					.collect(Collectors.toList());
		} catch (ApiException e) {
			throw new DiscoveryException("Error calling kube api", e);
		}
	}
	
	@Override
	public List<String> listNamespaces() {
		try {
			V1NamespaceList spaces = api.listNamespace(null, null, null, null, null, null, null, null);
			return spaces.getItems().stream().map(ns->ns.getMetadata().getName()).collect(Collectors.toList());
		} catch (ApiException e) {
			throw new DiscoveryException("Kube API problem: ",e);
		}
		
		
	}
	
	@Override
	public List<ClusterMember> listPods(String namespace) {
		try {
			return api.listNamespacedPod(namespace, null, null, null, null, null, null, null, null).getItems()
					.stream()
					.map(e->new ClusterMemberImpl(e))
					.collect(Collectors.toList()); 
		} catch (ApiException e) {
			e.printStackTrace();
			throw new DiscoveryException("Kubernetes API Error", e);
		}
	}
	
	@Override
	public List<String> discoverPods(String namespace, String cluster, int containerPort) {
		List<String> clusterMembers = new ArrayList<>();
		try {
			V1PodList list = api.listNamespacedPod(namespace, null, null, null, null, null, null, null, null); 
			for (V1Pod pod : list.getItems()) {
				String hostIp = pod.getStatus().getPodIP();
				for (V1Container container : pod.getSpec().getContainers()) {
					final List<V1EnvVar> env = container.getEnv();
					Optional<String> clusterName = env==null ? Optional.empty() : Optional.ofNullable(env.stream().filter(e -> e.getName().equals("CLUSTER")).map(e->e.getValue()).collect( Collectors.joining(",")));
					if(clusterName.isPresent() && !"".equals(clusterName.get())) {
						if(clusterName.get().equals(cluster)) {
							clusterMembers.add(hostIp+":"+containerPort);
						}
					}
				}
			}
		} catch (ApiException e) {
			throw new DiscoveryException("Kubernetes API Error", e);
		}
		return clusterMembers;
	}

    @Override
    public List<Deployment> listDeployments(String namespace) {
        try {
//        	appsApi.listD
        	return appsApi.listNamespacedDeployment(namespace, "", "", "", "",0, null, 10, false)
        			.getItems()
        			.stream()
        			.map(dep -> {
		                String name = dep.getMetadata().getName();
		                Map<String, String> templateLabels = dep.getSpec().getTemplate().getMetadata().getLabels();
		                Map<String, String> labels = dep.getMetadata().getLabels();
		                List<Event> events = listEvents(dep, name, templateLabels, labels);
		                return new DeploymentImpl(name, labels, templateLabels, dep, events);
		            }).collect(Collectors.toList());
        } catch (ApiException e) {
            throw new DiscoveryException("Error calling kube api", e);
        }
    }
    
    @Override
    public List<StatefulSet> listStatefulSets(String namespace) {
        try {
        	return appsApi.listNamespacedStatefulSet(namespace, "", "", "", "", 10, null, 10, false)
//        	return appsApi.listNamespacedDeployment(namespace, "", "", "", true, "", null, "0", 10, false)
        			.getItems().stream().map(dep -> {
                String name = dep.getMetadata().getName();
                Map<String, String> templateLabels = dep.getSpec().getTemplate().getMetadata().getLabels();
                Map<String, String> labels = dep.getMetadata().getLabels();
                List<Event> events = listStatefulSetEvents(dep, name, templateLabels, labels);
                return new StatefulSetImpl(name, labels, templateLabels, dep, events);
            }).collect(Collectors.toList());
        } catch (ApiException e) {
            throw new DiscoveryException("Error calling kube api", e);
        }
    }
    
    @Override
    public List<ReplicaSet> listReplicaSets(String namespace) {
        try {
        	return appsApi.listNamespacedReplicaSet(namespace, "true", null, null, null, 0, null, 10, null)
        		.getItems()
        		.stream()
        		.map(e->new ReplicaSetImpl(e))
        		.collect(Collectors.toList());
        } catch (ApiException e) {
            throw new DiscoveryException("Error calling kube api", e);
        }
    }
    
    

private List<Event> listEvents(V1Deployment dep, String name, Map<String, String> templateLabels,
		Map<String, String> labels) {
	List<Event> events = new ArrayList<>();
	                try {
	                	List<V1Event> list = api.listNamespacedEvent(dep.getMetadata().getNamespace(), "", "", "", "", 200, "", 100, false).getItems();
	                    for (V1Event event : list) {
	                    	if (event.getSource().getComponent().equals("deployment-controller")
	                                && event.getInvolvedObject().getName().equals(name)) {
	                            EventImpl ev = new EventImpl(event.getMetadata().getUid(), event.getMessage(), event.getReason(),
	                                    event.getType(), event.getMetadata().getNamespace(), event.getSource().getComponent());
	                            events.add(ev);
	                        }
	                    }
	                } catch (ApiException e) {
	                    logger.error("Could not parse events for this deployment", e);
	                }
	return events;
}

private List<Event> listStatefulSetEvents(V1StatefulSet dep, String name, Map<String, String> templateLabels,
		Map<String, String> labels) {
	List<Event> events = new ArrayList<>();
	                try {
	                	List<V1Event> list = api.listNamespacedEvent(dep.getMetadata().getNamespace(), "", "", "", "", 200, "", 100, false).getItems();
	                    for (V1Event event : list) {
	                    	if (event.getSource().getComponent().equals("deployment-controller")
	                                && event.getInvolvedObject().getName().equals(name)) {
	                            EventImpl ev = new EventImpl(event.getMetadata().getUid(), event.getMessage(), event.getReason(),
	                                    event.getType(), event.getMetadata().getNamespace(), event.getSource().getComponent());
	                            events.add(ev);
	                        }
	                    }
	                } catch (ApiException e) {
	                    logger.error("Could not parse events for this deployment", e);
	                }
	return events;
}



	@Override
	public String currentNamespace() {
		if(this.namespace!=null) {
			return this.namespace;
		}
		return System.getenv("NAMESPACE");
		
	}
}
