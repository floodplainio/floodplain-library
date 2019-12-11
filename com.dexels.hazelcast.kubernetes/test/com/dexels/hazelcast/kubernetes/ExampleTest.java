package com.dexels.hazelcast.kubernetes;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.dexels.hazelcast.kubernetes.impl.DiscoverPodsImpl;
import com.google.common.reflect.TypeToken;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Namespace;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1StatefulSetList;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;

public class ExampleTest {

	@Test @Ignore
	public void testSimple() throws IOException, ApiException {
//		String cluster = "test";
//		int containerPort = 5701;
		DiscoverPodsImpl dpi = new DiscoverPodsImpl();
		dpi.activate();
		if(!new File(System.getProperty("user.home")+"/.kube").exists()) {
			System.err.println("No kube config, skipping test");
			return;
		}
		dpi.listNamespaces()
			.stream()
			.flatMap(e->dpi.listPods(e).stream())
			.filter(e->e.labels().get("rackermon")!=null)
			.forEach(e->System.err.println("Cluster: "+e));
//		dpi.listPods("test").forEach(e->System.err.println(":: "+e));
	}
	
	
	@SuppressWarnings("serial")
	@Test(expected=RuntimeException.class) @Ignore
	public void testWatch() throws ApiException, IOException {
        ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);

        CoreV1Api api = new CoreV1Api();
        Watch<V1Namespace> watch = Watch.createWatch(
                client,
//                api.listPod
                api.listNamespaceCall(null, null, null, null, null, null, null, Boolean.TRUE, null, null),
                new TypeToken<Watch.Response<V1Namespace>>(){}.getType());

        for (Watch.Response<V1Namespace> item : watch) {
            System.out.printf("%s : %s%n", item.type, item.object.getMetadata().getName());
        }
//        watch.
	}
	
	@Test @Ignore
	public void testLab() throws IOException {
//		String cluster = "test";
//		int containerPort = 5701;
		DiscoverPodsImpl dpi = new DiscoverPodsImpl();
		dpi.activate();
		if(!new File(System.getProperty("user.home")+"/.kube").exists()) {
			System.err.println("No kube config, skipping test");
			return;
		}
		int size = dpi.listPods("lab-frank").size();
		System.err.println("size: "+size);
			dpi.listPods("lab-frank").stream()
//			.filter(e->e.labels().get("rackermon")!=null)
			.forEach(e->System.err.println("Cluster: "+e));
//		dpi.listPods("test").forEach(e->System.err.println(":: "+e));
	}
	
	
	@Test @Ignore
	public void testServices() throws IOException, ApiException {
		DiscoverPodsImpl dpi = new DiscoverPodsImpl();
		dpi.activate();
		if(!new File(System.getProperty("user.home")+"/.kube").exists()) {
			System.err.println("No kube config, skipping test");
			return;
		}
		dpi.listServices("test")
			.stream()
			.filter(e->e.labels().get("app")!=null)
			.filter(e->e.labels().get("rackermon")!=null)
			.forEach(e->System.err.println("Adding service "+e.name()+" -> "+e.labels().get("app")));
	}
	
	@Test @Ignore
	public void testDeployments() throws IOException, ApiException {
		DiscoverPodsImpl dpi = new DiscoverPodsImpl();
		dpi.activate();
		if(!new File(System.getProperty("user.home")+"/.kube").exists()) {
			System.err.println("No kube config, skipping test");
			return;
		}
		dpi.listDeployments("test")
			.stream()
//			.filter(e->e.labels().get("app")!=null)
//			.filter(e->e.labels().get("rackermon")!=null)
			.forEach(e->System.err.println("Adding deployments "+e.name()+" -> "+e.labels().get("app")));
	}

	@Test @Ignore
	public void test() throws IOException, ApiException {
		String namespace = "test";
		ApiClient client = Config.defaultClient();
		Configuration.setDefaultApiClient(client);
		CoreV1Api api = new CoreV1Api();
		V1PodList list = api.listNamespacedPod(namespace, null, null, null, null, null, null, null, null); 
		for (V1Pod pod : list.getItems()) {
			System.out.println(pod.getMetadata().getName() + " -> " + pod.getMetadata().getClusterName() + " ->> "
					+ pod.getStatus().getHostIP());
			pod.getSpec().getContainers().stream().map(e->e.getPorts()).forEach(e->System.err.println("Ports: "+e));

		}
	}


	@Test @Ignore
	public void testStateFul() throws IOException, ApiException {
		String namespace = "test";
//		String url = "https://phoebe.sportlink-infra.net:6443";
//		ApiClient client = Config.fromToken(url, "",false);
		ApiClient client = Config.defaultClient();
		Configuration.setDefaultApiClient(client);
        AppsV1Api appsApi = new AppsV1Api();
		V1StatefulSetList l = appsApi.listNamespacedStatefulSet(namespace,  null, null, null, null, null, null, null, null);
		l.getItems().forEach(e->System.err.println("Deployment: "+e.getMetadata().getName()));

//		for (V1StatefulSet element : l.getItems()) {
//			System.err.println("Stateful set: "+element.getSpec().getServiceName()+" element: "+element);
//		}
	}
	
	@Test @Ignore
	public void testListDeployments() throws ApiException, IOException {
		ApiClient client = Config.defaultClient();
		Configuration.setDefaultApiClient(client);
        AppsV1Api appsApi = new AppsV1Api();
    	appsApi.listNamespacedDeployment("test", "", "", "", "", 0, null, 10, false)
    			.getItems()
    			.stream()
    			.forEach(e->System.err.println("Deployment: "+e.getMetadata().getName()+"\n"+e.getMetadata().getLabels()));
	}

	@Test @Ignore
	public void testTree() throws IOException, ApiException {
		DiscoverPodsImpl dpi = new DiscoverPodsImpl();
		dpi.activate();
		if(!new File(System.getProperty("user.home")+"/.kube").exists()) {
			System.err.println("No kube config, skipping test");
			return;
		}
			// only test for now
		String namespace = "test";
		List<Deployment> depl = dpi.listDeployments(namespace);
		List<ReplicaSet> replicaSet = dpi.listReplicaSets(namespace);
		List<StatefulSet> statefulSets = dpi.listStatefulSets(namespace);
		List<ClusterMember> pods = dpi.listPods(namespace);

		
		dpi.listDeployments("test")
			.stream()
//			.filter(e->e.labels().get("app")!=null)
//			.filter(e->e.labels().get("rackermon")!=null)
			.forEach(e->System.err.println("Adding deployments "+e.name()+" -> "+e.labels().get("app")));
	}
}

