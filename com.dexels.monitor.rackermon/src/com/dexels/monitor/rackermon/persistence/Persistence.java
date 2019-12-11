package com.dexels.monitor.rackermon.persistence;

import java.util.List;

import com.dexels.monitor.rackermon.ClusterAlert;
import com.dexels.monitor.rackermon.StateChange;
import com.dexels.monitor.rackermon.checks.api.ServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public interface Persistence {
	static final long MIN_ALERT_MS = 2 * 60 * 1000L; // 2 minutes
	static final long MAX_ALERT_MS = 24 * 60 * 60 * 1000L; // 24 hours

	public void storeStatus(String objectId, ServerCheckResult status);

	public void storeStateChange(StateChange change);

	public void storeClusterAlert(String name, ServerCheck failed);

	public void storeClusterClearedAlert(String name, ServerCheck checkInFailedCondition);

	public List<ClusterAlert> getHistoricClusterAlerts(String clusterName);

	/**
	 * 
	 * @param clusterName
	 * @return Returns a string with uptime for the past 1m/6m/1y
	 */
	public String getUptime(String clusterName);
}
