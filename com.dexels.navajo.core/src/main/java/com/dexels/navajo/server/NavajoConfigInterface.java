package com.dexels.navajo.server;

import com.dexels.navajo.server.enterprise.integrity.WorkerInterface;
import com.dexels.navajo.sharedstore.SharedStoreInterface;

public interface NavajoConfigInterface extends NavajoIOConfig {

	public static final int MAX_ACCESS_SET_SIZE = 50;

	// Read/write configuration.

	// Identity methods.
	public String getInstanceName();
	public String getInstanceGroup();
	
	// Available modules.
	public SharedStoreInterface getSharedStore();
	public ClassLoader getClassloader();
	
	public WorkerInterface getIntegrityWorker();
	
	// Statistics.
	public double getCurrentCPUload();
	
	 public boolean useLegacyDateMode();

   
	// Setters/getters.
	public int getMaxAccessSetSize();
	public float getAsyncTimeout();
	public void doClearCache();
	public void doClearScriptCache();
	public boolean isLockManagerEnabled();

	/**
	 * This one consults the configuration
	 * @return
	 */
	public boolean isEnableStatisticsRunner();
	/**
	 * This one asks the statisticsrunner
	 * @return
	 */
	public boolean isCompileScripts();
	

	public Object getParameter(String string);
	
}
