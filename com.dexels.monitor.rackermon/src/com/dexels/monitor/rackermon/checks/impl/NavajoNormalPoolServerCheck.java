package com.dexels.monitor.rackermon.checks.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public class NavajoNormalPoolServerCheck extends NavajoPoolServerCheck {
	private static final long serialVersionUID = -5030713164357446751L;

	@Override
    public String getName() {
        return "Normalpool status";
    }

    @Override
    public ServerCheckResult performCheck() {
        ServerCheckResult s = new ServerCheckResult(this);
        Optional<String> host = server.getEndPoint();
        if(!host.isPresent()) {
        	return new ServerCheckResult(this).setStatus(ServerCheckResult.STATUS_UNKNOWN);
        }
        Map<String, String> params = new HashMap<>();
        params.put("type", "normalpoolstatus");
        String response = performHttpPOSTCheck(host.get() + "/status", params);
        s.setRawResult(response);
        if (response.contains("error")) {
            s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
        } else {
            parsePoolResult(s, response);
        }
        return updateStatus(s);
    }
 
    @Override
    public int getClusterServiceFailureType() {
        return CLUSTER_FAILURE_ONE_SERVICE;
    }
    
    @Override
    public String getFailureDescription() {
        return "servicedelay";
    }
    
    @Override
    public Integer getServiceImpact() {
        return -10;
    }

}
