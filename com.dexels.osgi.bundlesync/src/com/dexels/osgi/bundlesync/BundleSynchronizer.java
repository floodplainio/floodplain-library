package com.dexels.osgi.bundlesync;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.wiring.BundleRevision;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.url.URLStreamHandlerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Component(configurationPolicy=ConfigurationPolicy.REQUIRE, name="dexels.bundles",service={})
public class BundleSynchronizer implements Runnable {
	
	private static final int SLEEPTIME = 10000;
	private Map<String, Object> settings;
	private final AtomicBoolean running = new AtomicBoolean(false);
	private final AtomicBoolean first = new AtomicBoolean(true);
	private Properties propertyFile = new Properties();
	
	private static final Logger logger = LoggerFactory
			.getLogger(BundleSynchronizer.class);
	
	private Thread updateThread = null;
	private String cacheDir = null;
	private final Set<String> owned = new HashSet<>();
	private BundleContext bundleContext = null;

	private final Set<String> compileTimeBundles = new HashSet<>();
	private final Set<String> bootBundles = new HashSet<>();
	private final Set<String> runtimeBundles = new HashSet<>();
	private URLStreamHandlerService mvnHandler;


	private enum BundleCategory {
		COMPILE,BOOT,RUNTIME
	}
	
	@Activate
	public synchronized void activate(BundleContext bundleContext, Map<String,Object> settings) throws IOException {
		this.settings = settings;
		this.bundleContext  = bundleContext;
		String disableBundleSync = System.getenv("NO_BUNDLESYNC");
		if(disableBundleSync!=null && !"".equals(disableBundleSync) && !"false".equalsIgnoreCase(disableBundleSync)) {
			logger.info("NO_BUNDLESYNC set, so not starting bundle synchronizer");
			return;
		}
		logger.info("Starting Bundle Synchronizer: "+this.first.get());

		updateThread = new Thread(this,"Bundle Synchronizer");
		this.cacheDir  = (String) settings.get("cacheDir");
		loadOwnedBundles();
		logger.info("Preload owned bundles: "+owned);
		this.running.set(true);;
		updateThread.start();
	}
	
	@Modified
	public synchronized void modified(Map<String,Object> settings ) {
		this.settings = settings;
	}
	
	

	private synchronized Set<String> getBundles(BundleCategory category) {
		
		final Set<String> result = new HashSet<>();
		for (Entry<String,Object> entry : settings.entrySet()) {
			String key = entry.getKey();
			if(!(entry.getValue() instanceof String)) {
				continue;
			}
			String value = (String)entry.getValue();
			switch (category) {
				case COMPILE:
					if(key.startsWith("compile.")) {
						logger.info("Installing compile time bundle: {}", value);
						result.addAll(Arrays.asList(value.split(",")));
						compileTimeBundles.addAll(Arrays.asList(value.split(",")));
					}
					break;
				case BOOT:
					if("bootrepo".equals(key) || key.startsWith("boot.")) {
						logger.info("Installing boot bundle: {}", value);
						result.addAll(Arrays.asList(value.split(",")));
						bootBundles.addAll(Arrays.asList(value.split(",")));
					}
					break;
				case RUNTIME:
					if("repo".equals(key) || key.startsWith("runtime.")) {
						logger.info("Installing realtime bundle: {}", value);
						result.addAll(Arrays.asList(value.split(",")));
						runtimeBundles.addAll(Arrays.asList(value.split(",")));

					}
					break;
				default:
					break;
			}
		}
		return result;
	}
	
	private void loadOwnedBundles() throws IOException {
		if(cacheDir==null) {
			return;
		}
		File cacheFolder = new File(cacheDir);
		if(!cacheFolder.exists()) {
			cacheFolder.mkdirs();
		}
		File ownershipFile = new File(cacheFolder,"ownedbundles.properties");
		if(ownershipFile.exists()) {
			try(FileReader fr = new FileReader(ownershipFile)) {
				propertyFile.load(fr);
				String rep = propertyFile.getProperty("ownedbundles");
				owned.clear();
				if(rep!=null) {
					String[] reps = rep.split(",");
					for (String r : reps) {
						owned.add(r);
					}
				}
			}
			
		}
	}
	
	
	private void saveOwnedBundles() throws IOException {
		if(cacheDir==null) {
			return;
		}
		File cacheFolder = new File(cacheDir);
		File ownershipFile = new File(cacheFolder,"ownedbundles.properties");
		boolean firstFound = true;
		StringBuilder sb = new StringBuilder();
		for (String uri : owned) {
			if(!firstFound) {
				sb.append(',');
			} else {
				firstFound = false;
			}
			sb.append(uri);
		}
		propertyFile.setProperty("ownedbundles", sb.toString());
		try (FileWriter fw = new FileWriter(ownershipFile)) {
			propertyFile.store(fw, "Owned by the bundle synchronizer");
		}
	}

	@Deactivate
	public void deactivate() {
		this.running.set(false);;
		if(updateThread!=null) {
			updateThread.interrupt();
		}
	}
	
	@Reference(target="(url.handler.protocol=mvn)",unbind="clearMvnHandler",policy=ReferencePolicy.DYNAMIC)
	public void setMvnHandler(URLStreamHandlerService handlerService) {
		this.mvnHandler = handlerService;
	}

	public void clearMvnHandler(URLStreamHandlerService handlerService) {
		this.mvnHandler = null;
	}

	private synchronized void updateBundles(Set<String> bundleLocations) {
		if(bundleLocations==null || bundleLocations.isEmpty()) {
			logger.debug("No locations mentioned. Leaving bundles alone.");
			return;
		}
		int frameworkState = bundleContext.getBundle(0).getState();
		if(frameworkState==Bundle.STOPPING || frameworkState==Bundle.UNINSTALLED) {
			logger.warn("Framework not healthy, not touching it.");
			return;
		}
		final Set<Bundle> added = new HashSet<>();

		final Map<String,Bundle> present = new HashMap<>();
		Bundle[] bundles = bundleContext.getBundles();
		for (Bundle b : bundles) {
			present.put(b.getLocation(),b);
		}
		final Set<String> encountered = new HashSet<>();
		final Set<String> retrySet = new HashSet<>();
		for (String location : bundleLocations) {
			try {
				encountered.add(location);
				if(!present.containsKey(location)) {
					// owned should be persistent
					owned.add(location);
					Bundle installed = bundleContext.installBundle(location);
					added.add(installed);
					logger.info("Adding bundle: {}",location);
				} else {
					logger.debug("bundle: {} is already present",location);

				}
			} catch (BundleException e) {
				logger.error("BundleException installing bundle, continuing ", e);
				retrySet.add(location);
			} catch (Throwable e) {
				logger.error("Installing bundle, continuing ", e);
			}
		}
		processRetries(retrySet);
		if(!added.isEmpty()) {
			logger.info("# of added bundles: {}",added.size());
		}
		for (Bundle b : added) {
			int state = b.getState();
			if(state!=Bundle.RESOLVED) {
				logger.debug("Unresolved bundle # {} : {}",b.getBundleId(),b.getSymbolicName());
			}
		}
		logger.debug("Owned:{}", owned);
		Set<String> orphaned = new HashSet<>(owned);
		orphaned.removeAll(encountered);
		orphaned.removeAll(compileTimeBundles);
		orphaned.removeAll(bootBundles);
		if(!orphaned.isEmpty()) {
			logger.info("# of added bundles to remove: {}", orphaned.size());
		}
		for (String uri : orphaned) {
			try {
				logger.info("Bundle with uri: {} is owned, and is now orphaned. Will remove.",uri);
				owned.remove(uri);
				Bundle toBeRemoved = bundleContext.getBundle(uri);
				if(toBeRemoved!=null) {
					toBeRemoved.uninstall();
					owned.remove(uri);
				} else {
					logger.warn("Can't find bundle with location: {}, assuming that is gone already",uri);
				}
			} catch (Exception e) {
				logger.error("Error removing bundle: ", e);
			}
		}

		for (Bundle toStart : added) {
			boolean isFragment = (toStart.adapt(BundleRevision.class).getTypes() & BundleRevision.TYPE_FRAGMENT) != 0;
			if(!isFragment) {
				try {
					logger.info("Starting bundle: {} at: {}", toStart.getBundleId(), toStart.getLocation());
					toStart.start();
				} catch (BundleException e) {
					logger.error("Failed to start bundle: {} at: {}",toStart.getBundleId(),toStart.getLocation(),e);
				}
			} else {
				logger.info("Skipping fragment: {} at: {}",toStart.getBundleId(),toStart.getLocation());
			}
		}
		try {
			saveOwnedBundles();
		} catch (IOException e) {
			logger.error("Error saving: ", e);
		}
		logger.debug("All done");
	}

	private void processRetries(Set<String> retrySet) {
		Set<String> failSet = new HashSet<>();
		int originalSize = retrySet.size();
		while(!retrySet.isEmpty() && failSet.size()<originalSize) {
			for (String location : retrySet) {
				try {
					bundleContext.installBundle(location);
					logger.debug("Installed location after retry: {}", location);
					retrySet.remove(location);
					break;
				} catch (BundleException e) {
					logger.error("Error retrying: ", e);
					failSet.add(location);
				}
			}
		}
	}

	@Override
	public void run() {
		try {
			while(running.get()) {
			    int systemBundleState = bundleContext.getBundle(0).getState();
			    if (systemBundleState == Bundle.ACTIVE) {
    				try {
    					Set<String> resolutionSet = new HashSet<>();
    					if(first.get()) {
    						resolutionSet.addAll(getBundles(BundleCategory.COMPILE));
    						resolutionSet.addAll(getBundles(BundleCategory.BOOT));
    						first.set(false);
    					}
    					resolutionSet.addAll(getBundles(BundleCategory.RUNTIME));
    					logger.debug("Starting bundlesync from thread: {}", Thread.currentThread().getName());
    					long now = System.currentTimeMillis();
    					updateBundles(resolutionSet);
    					long elapsed = System.currentTimeMillis() - now;
    					logger.debug("Bundle sync iteration took: {} millis.",elapsed);
    					checkResolvedAndRunning(new String[]{(String) settings.get("bootrepo"),(String) settings.get("repo")});
    				} catch (Throwable e) {
    					logger.error("Error: ", e);
    					try {
    						Thread.sleep(SLEEPTIME);
    					} catch (InterruptedException e1) {
    						logger.trace("Silent",e1);
    					}
    				}
			    } else {
			        logger.warn("SystemBundle not active - not doing anything! {}", systemBundleState);
			    }
				try {
					Thread.sleep(SLEEPTIME);
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
					logger.trace("Silent",e1);
				} catch (Throwable e) {
					logger.error("Error: ", e);
				}
			}
		} catch (Throwable e) {
			logger.error("Generic error: ", e);
		} finally {
			logger.info("BundleSync exiting");
		}
	}

	private void checkResolvedAndRunning(String[] locationList) throws BundleException {
		List<Bundle> toResolve = new ArrayList<>();
		for (String bundleLocations : locationList) {
			if(bundleLocations!=null) {
				final String[] bundleLocationList = bundleLocations.split(",");
				for (String location : bundleLocationList) {
					Bundle b = bundleContext.getBundle(location);
					// mostly for shutdowns
					if(b!=null) {
						if(b.getState()==Bundle.INSTALLED) {
							toResolve.add(b);
						}
					}
				}
			}
		}

		for (Bundle bundle : toResolve) {
//			bundleContext.
			logger.info("Bundle is not resolved: {} :: {}",bundle.getSymbolicName(),bundle.getBundleId());
		}

		List<Bundle> toStart = new ArrayList<>();
		for (String bundleLocations : locationList) {
			if (bundleLocations != null) {
				final String[] bundleLocationList = bundleLocations.split(",");

				for (String location : bundleLocationList) {
					Bundle b = bundleContext.getBundle(location);
					// mostly for shutdowns
					if(b!=null) {
						if (b.getState() == Bundle.RESOLVED) {
							toStart.add(b);
						}
					}
				}
			}
		}
		for (Bundle bundle : toStart) {
			logger.info("Bundle is not started... Starting :"
					+ " {} :: {}",bundle.getSymbolicName(),bundle.getBundleId());
			bundle.start();
		}
}

}
