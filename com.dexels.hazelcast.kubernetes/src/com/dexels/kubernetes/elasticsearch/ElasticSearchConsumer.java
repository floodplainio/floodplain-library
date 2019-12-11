package com.dexels.kubernetes.elasticsearch;

import java.util.HashSet;
import java.util.Set;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

@Component(enabled=false)
public class ElasticSearchConsumer {
	
	private final Set<ElasticSearchPod> pods = new HashSet<>();
	
	@Activate
	public void activate() {
		System.err.println("Activating elastic search consumer");
	}
	
	@Deactivate
	public void deactivate() {
		System.err.println("Deactivating elastic search consumer");
	}
	
	@Reference(unbind="clearElasticSearchPod",policy=ReferencePolicy.DYNAMIC,cardinality=ReferenceCardinality.AT_LEAST_ONE)
	public void setElasticSearchPod(ElasticSearchPod pod) {
		pods.add(pod);
		System.err.println("Number of pods is now: "+pods.size());
	}
	
	public void clearElasticSearchPod(ElasticSearchPod pod) {
		pods.remove(pod);
		System.err.println("Number of pods is now: "+pods.size());
	}
}
