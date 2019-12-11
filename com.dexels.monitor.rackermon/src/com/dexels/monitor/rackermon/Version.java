package com.dexels.monitor.rackermon;

import java.util.Dictionary;
import java.util.Hashtable;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class Version implements BundleActivator {

	private static BundleContext bundleContext;
	private ServiceRegistration<ClassLoader> classLoaderService;
	

	public Version() {
	}

	@Override
	public void start(BundleContext context) throws Exception {
		bundleContext = context;
		Dictionary<String, Object> settings = new Hashtable<String, Object>();
		settings.put("tag", "hazelcast");
		classLoaderService = bundleContext.registerService(ClassLoader.class, getClass().getClassLoader(), settings);

	}

	@Override
	public void stop(BundleContext arg0) throws Exception {
		if(classLoaderService!=null) {
			this.classLoaderService.unregister();
		}


	}

}