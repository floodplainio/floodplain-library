package com.dexels.server.mgmt.status;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.runtime.ServiceComponentRuntime;
import org.osgi.service.component.runtime.dto.ComponentConfigurationDTO;
import org.osgi.service.component.runtime.dto.ComponentDescriptionDTO;

import io.reactivex.Observable;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;

public class OSGiStatus {
	public static List<String> activationFailed(BundleContext context,ServiceComponentRuntime serviceComponentRuntime) {
		return listComponents(context,serviceComponentRuntime, e->e.state==ComponentConfigurationDTO.FAILED_ACTIVATION, component->"Component Activation Failed: "+component.failure)
			.collect(()->new ArrayList<String>(), (sb,s)->sb.add(s))
			.blockingGet();
	}
	
	public static List<String> unsatisfiedReference(BundleContext context,ServiceComponentRuntime serviceComponentRuntime) {
		final ArrayList<String> list = listComponents(context,serviceComponentRuntime,e->{ 
				return e.state==ComponentConfigurationDTO.UNSATISFIED_REFERENCE;}, component->"Component Unsatisfied Reference: "+component.failure)
			.collect(()->new ArrayList<String>(), (sb,s)->sb.add(s))
			.blockingGet();
		System.err.println("REFERENCEISSUES: "+list);
		return list;
	}

	
	public static Observable<String> listComponents(BundleContext context,ServiceComponentRuntime serviceComponentRuntime, Predicate<ComponentConfigurationDTO> filter, Function<ComponentConfigurationDTO,String> map) {
		return Observable.fromArray(context.getBundles())
			.flatMap(bundle->Observable.fromIterable(listComponents(serviceComponentRuntime, bundle)))
			.flatMap(component->Observable.fromIterable(serviceComponentRuntime.getComponentConfigurationDTOs(component)))
			.filter(filter)
			.map(map);
	}

	private static Collection<ComponentDescriptionDTO> listComponents(ServiceComponentRuntime serviceComponentRuntime, Bundle bundle) {
		if (bundle.getState() != Bundle.ACTIVE) {
			return Collections.emptyList();
		}
		return serviceComponentRuntime.getComponentDescriptionDTOs(bundle);

	}
}
