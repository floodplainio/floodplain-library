package com.dexels.monitor.rackermon.checks.impl;

import java.util.Optional;

import com.dexels.monitor.rackermon.checks.api.AbstractServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public class StatusServletServerCheck extends AbstractServerCheck {
	private static final long serialVersionUID = 7547015832381201704L;


	@Override
    public String getName() {
        return "Status Servlet";
    }

    @Override
    public ServerCheckResult performCheck() {
        ServerCheckResult s = new ServerCheckResult(this);
        try {
            Optional<String> host = server.getEndPoint();
            if(!host.isPresent()) {
            	return new ServerCheckResult(this).setStatus(ServerCheckResult.STATUS_UNKNOWN);
            }
            String response = performHttpGETCheck(host.get() + "/status");
            s.setRawResult(response);
            if (response.contains("error")) {
                s.setStatus(ServerCheckResult.STATUS_ERROR);
            }
        } catch (Exception e) {
            // something went wrong! Go to unknown state
            s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
            s.setRawResult("Exception");
        }

        return updateStatus(s);
    }

    @Override
    public int getMinimumFailedCheckCount() {
        return 5;
    }
    
    @Override
    public String getFailureDescription() {
        return "serviceunavailable";
    }
    
  
    @Override
    public Integer getServiceImpact() {
    	// Status servlet has a very high impact 
    	return -1000;
    }

    @Override
    public int getClusterServiceFailureType() {
        return CLUSTER_FAILURE_ALL_SERVICES;
    }
    
    @Override
    public boolean unknownIsFailed() {
        return true;
    }
}
