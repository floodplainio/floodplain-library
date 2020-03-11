package com.dexels.navajo.version;


import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public  class AbstractVersion implements BundleActivator {

	private static final Logger logger = LoggerFactory.getLogger(AbstractVersion.class);
	protected BundleContext context = null;
	
	@Override
	public void start(BundleContext bc) throws Exception {
		context = bc;
		if(bc==null) {
			logger.debug("Bundle started in non-osgi environment: {}",getClass().getName());
		}
	}

	
	
	public static boolean osgiActive() {
		return false;
	}
	
	
	@Override
	public void stop(BundleContext arg0) throws Exception {
		context = null;
		
	}

	public void shutdown() {
		if(context==null) {
			logger.info("No OSGi present.");

		}
		logger.info("Shutting down bundle: {}",getClass().getName());
	}
}
