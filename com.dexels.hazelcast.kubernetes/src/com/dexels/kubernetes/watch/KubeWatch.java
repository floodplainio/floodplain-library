package com.dexels.kubernetes.watch;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ContainerStatus;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

@Component(enabled=false)
public class KubeWatch {
	private ApiClient client;
	private CoreV1Api coreV1Api;
	private String namespace;

	private ConfigurationAdmin configurationAdmin;

	private final static Logger logger = LoggerFactory.getLogger(KubeWatch.class);
	private Disposable podsDisposable = null;
	private Disposable serviceDisposable = null;

	@Activate
	public void activate() throws IOException {
		
		this.namespace = "test";
			client = Config.defaultClient();
			client.getHttpClient().setReadTimeout(0, TimeUnit.MILLISECONDS);
			io.kubernetes.client.Configuration.setDefaultApiClient(client);
			coreV1Api = new CoreV1Api();
			this.podsDisposable = watchPodsReactive()
				.subscribe(this::consumePodEvent);
			this.serviceDisposable = watchServicesReactive()
				.subscribe(this::consumeServiceEvent);
	}

	@SuppressWarnings("unchecked")
	private Watch.Response<V1Pod> cast(Response<? extends Object> o) {
		return (Watch.Response<V1Pod>)o ;
	}

	@SuppressWarnings("unchecked")
	private Watch.Response<V1Service> castService(Response<? extends Object> o) {
		return (Watch.Response<V1Service>)o ;
	}

	@SuppressWarnings("serial")
	private Flowable<Response<V1Pod>> watchPodsReactive() {
		try {
			return Flowable.fromIterable(Watch.createWatch(client, coreV1Api.listNamespacedPodCall(namespace,null, null, null, null,
					null, null, null, Boolean.TRUE, null, null), new TypeToken<Watch.Response<V1Pod>>() {
					}.getType()))
					.map(e->this.cast(e))
					.doOnSubscribe(e->System.err.println("Starting"))
					.doOnNext(e->System.err.println("Event"))
					.subscribeOn(Schedulers.io());
			
		} catch (ApiException e) {
			return Flowable.error(e);
		}
	}

	@SuppressWarnings("serial")
	private Flowable<Response<V1Service>> watchServicesReactive() {
		try {
			return Flowable.fromIterable(Watch.<V1Service>createWatch(client, coreV1Api.listNamespacedServiceCall(namespace, null, null, null, null, null, null, null, null, null, null), new TypeToken<Watch.Response<V1Service>>() {
					}.getType()))
					.map(e->this.castService(e))
					.doOnSubscribe(e->System.err.println("Starting"))
					.doOnNext(e->System.err.println("Event"))
					.subscribeOn(Schedulers.io());
			
		} catch (ApiException e) {
			return Flowable.error(e);
		}
	}
	
	@Reference(unbind = "clearConfigurationAdmin", policy = ReferencePolicy.DYNAMIC)
	public void setConfigurationAdmin(ConfigurationAdmin configurationAdmin) {
		this.configurationAdmin = configurationAdmin;
	}

	@Reference(cardinality=ReferenceCardinality.AT_LEAST_ONE)
	public void clearConfigurationAdmin(ConfigurationAdmin a) {
		this.configurationAdmin = null;
	}

//	public class WatchHandler implements Runnable {
//		
//		public WatchHandler() {}
//		@Override
//		public void run() {
//			while (true) {
//				try {
//					watch.forEach(response -> {
//						consumePodEvent(response);
//					});
//				} catch (Throwable e) {
//					logger.error("Error: ", e);
//					try {
//						Thread.sleep(1000 * 5);
//					} catch (InterruptedException e1) {
//						logger.error("Error: ", e1);
//					}
//				}
//			}
//		}
//
//	}

	private void consumeServiceEvent(Response<V1Service> response) {
		String ip = response.object.getSpec().getClusterIP();
//		String clusterIp = response.object.getSpec().getClusterIP();
		System.err.println("Externalip: "+ip);
		int port = response.object.getSpec().getPorts().stream().map(e->e.getPort()).findFirst().orElse(-1);
		System.err.println("Port: "+port);
		String name = response.object.getMetadata().getName();
		Map<String,String> selectors = response.object.getSpec().getSelector();
		Map<String,String> labels = response.object.getMetadata().getLabels();
		Dictionary<String, Object> settings = new Hashtable<>();
		if(labels!=null) {
			labels.entrySet().stream().forEach(e->settings.put(e.getKey(), e.getValue()));
		}
		settings.put("name", name);
		String pid = getServicePid(name);
		Configuration cc;
		try {
			cc = createOrReuse(pid, getFilter(name));
			switch(response.type) {
			case "ADDED":
			case "MODIFIED":
				if(ip!=null) {
					settings.put("host", ip);
				}
				settings.put("port", port);
				try {
					if(cc!=null) {
						updateIfChanged(cc, settings);
					}
				} catch (Exception e1) {
					logger.error("Error: ", e1);
				}
				break;
			case "DELETED":
				try {
					if(cc!=null) {
						cc.delete();
					}
				} catch (Exception e1) {
					logger.error("Error: ", e1);
				}
			}
		} catch (Exception e2) {
			e2.printStackTrace();
		}				
	}
	
	private void consumePodEvent(Response<V1Pod> response) {
		final V1PodStatus status = response.object.getStatus();
		String name = response.object.getMetadata().getName();
		System.err.println("Name: "+name);
		System.err.println("Status: "+status.getPhase());
		Optional<V1ContainerStatus> errorState = status.getContainerStatuses().stream().filter(e->!e.isReady()).findAny();
		
		if(errorState.isPresent()) {
			System.err.println("Error detected: "+errorState.get());
		}
		String podIp = status.getPodIP();
		List<Integer> ports = response.object.getSpec()
				.getContainers().stream().map(e -> e.getPorts()).filter(e -> e != null)
				.flatMap(e -> e.stream()).map(e -> e.getContainerPort()).collect(Collectors.toList());

		Map<String,String> labels = response.object.getMetadata().getLabels();
		Dictionary<String, Object> settings = new Hashtable<>();
		labels.entrySet().stream().forEach(e->settings.put(e.getKey(), e.getValue()));
		String pid = getPid(labels);
		settings.put("name", name);
		Configuration cc;
		try {
			cc = createOrReuse(pid, getFilter(response.object.getMetadata().getName()));
			if(errorState.isPresent()) {
				cc.delete();
			} else {
				switch(response.type) {
				case "ADDED":
				case "MODIFIED":
					if(podIp!=null) {
						settings.put("host", podIp);
					}
					final Integer firstPort = ports.stream().findFirst().orElse(-1);
					if(firstPort!=null && firstPort > 0) {
						settings.put("port", firstPort);
					}
					try {
						if(cc!=null) {
							updateIfChanged(cc, settings);
						}
					} catch (Exception e1) {
						logger.error("Error: ", e1);
					}
					break;
				case "DELETED":
					try {
						if(cc!=null) {
							cc.delete();
						}
					} catch (Exception e1) {
						logger.error("Error: ", e1);
					}
				}
			}

		} catch (Exception e2) {
			e2.printStackTrace();
		}
	}

	private String getFilter(String name) {
		return "(name="+name+")";
	}
	private String getPid(Map<String,String> labels) {
		String type = labels.get("type");
		return "osgi.kube.pod."+type;
	}

	private String getServicePid(String name) {
//		String type = labels.get("type");
		return "osgi.kube.service."+name;
	}

	
	protected Configuration createOrReuse(String pid, final String filter) throws IOException {
		if(this.configurationAdmin==null) {
			System.err.println("Ignoring. No configadmin");
			return null;
		}
		Configuration cc = null;
		try {
			Configuration[] c = configurationAdmin.listConfigurations(filter);
			if (c != null && c.length > 1) {
				logger.warn("Multiple configurations found for filter: {}", filter);
			}
			if (c != null && c.length > 0) {
				cc = c[0];
			}
		} catch (InvalidSyntaxException e) {
			logger.error("Error in filter: {}", filter, e);
		}
		if (cc == null) {
			cc = configurationAdmin.createFactoryConfiguration(pid, null);
			// resourcePids.add(cc.getPid());
		}
		return cc;
	}

	private void updateIfChanged(Configuration c, Dictionary<String, Object> settings) throws IOException {
		Dictionary<String, Object> old = c.getProperties();
		if (old != null) {
			if (!old.equals(settings)) {
				c.update(settings);
			}
		} else {
			c.update(settings);
		}
	}
	
	@Deactivate
	public void deactivate() {
		if(podsDisposable!=null) {
			podsDisposable.dispose();
		}
		if(serviceDisposable!=null) {
			serviceDisposable.dispose();
		}
	}
}
