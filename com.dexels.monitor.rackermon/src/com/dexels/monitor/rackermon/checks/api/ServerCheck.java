package com.dexels.monitor.rackermon.checks.api;

import java.util.Map;

import com.dexels.monitor.rackermon.Server;
import java.io.Serializable;


public interface ServerCheck extends Serializable {
	public static final int CLUSTER_FAILURE_NONE = 1;
	public static final int CLUSTER_FAILURE_ONE_SERVICE = 2;
	public static final int CLUSTER_FAILURE_ALL_SERVICES = 3;

	
    public String getName();
    public Server getServer();
    public void setServer(Server server);
    
    public boolean isTimeToRun();
	public ServerCheckResult performCheck();
    
    public int getMinimumFailedCheckCount();
    
    public Integer getServiceImpact();
    public int getClusterServiceFailureType();
    public String getFailureDescription();
    public boolean unknownIsFailed();

    public String performHttpGETCheck(String url);
    public String performHttpPOSTCheck(String url, Map<String, String> content);
    public String performHttpPOSTCheck(String url, String content, int timeout);
    public void setParameters(int minimumFailedCheckCount, int runIntervalInSeconds, String contentToCheck);
    
 
    
}


