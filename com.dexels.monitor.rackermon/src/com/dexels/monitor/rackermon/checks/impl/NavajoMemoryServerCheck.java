package com.dexels.monitor.rackermon.checks.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.dexels.monitor.rackermon.checks.api.AbstractServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public class NavajoMemoryServerCheck extends AbstractServerCheck {
	private static final long serialVersionUID = -3980493977906810907L;
	
	private static final int MEMORY_ERROR_LIMIT_PERCENTAGE = 97;
    private static final int MEMORY_WARNING_LIMIT_PERCENTAGE = 93;

    @Override
    public String getName() {
        return "Memory usage";
    }

    @Override
    public ServerCheckResult performCheck() {
        ServerCheckResult s = new ServerCheckResult(this);
        try {
            Optional<String> host = server.getEndPoint();
            if(!host.isPresent()) {
            	return new ServerCheckResult(this).setStatus(ServerCheckResult.STATUS_UNKNOWN);
            }
            Map<String, String> params = new HashMap<>();
            params.put("type", "memory");
            String response = performHttpPOSTCheck(host.get() + "/status", params);
            s.setRawResult(response);
            if (response.contains("error")) {
                s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
            } else {
                String[] splitted = response.split("/");
                if (splitted.length < 2) {
                    s.setStatus(ServerCheckResult.STATUS_UNKNOWN);

                } else {
                    Double used = Double.valueOf(splitted[0]);
                    Double max = Double.valueOf(splitted[1]);
                    if (((used / max) * 100) >= MEMORY_ERROR_LIMIT_PERCENTAGE) {
                        s.setStatus(ServerCheckResult.STATUS_ERROR);
                    } else if (((used / max) * 100) >= MEMORY_WARNING_LIMIT_PERCENTAGE) {
                        s.setStatus(ServerCheckResult.STATUS_WARNING);
                    }
                    // Use long for setting the rawResult to prevent getting x.104303483840 values...
                    long usedLong = Long.valueOf(splitted[0]);
                    long maxLong = Long.valueOf(splitted[1]);
                    s.setRawResult((usedLong / (1024 * 1024)) + "/" + (maxLong / (1024 * 1024)));
                }

            }
        } catch (Exception e) {
            // something went wrong! Go to unknown state
            s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
            s.setRawResult("Exception");
        }

        return updateStatus(s);
    }

    public static void main(String[] args) {

        Double used = Double.valueOf("3900");
        Double max = Double.valueOf("4000");
        if (((used / max) * 100) >= MEMORY_ERROR_LIMIT_PERCENTAGE) {
            System.out.println("err");
        } else if (((used / max) * 100) >= MEMORY_WARNING_LIMIT_PERCENTAGE) {
            System.out.println("warn");
        } else {
            System.out.println("OK!");
        }

    }
    
    @Override
    public int getMinimumFailedCheckCount() {
    	// Memory seems to fluctuate quite a bit - it can get quite high before
    	// the GC kicks in. Therefore, we need a high number of consecutive failed 
    	// checks before we actually sound the alarm
        return 12;
    }
    
    @Override
    public Integer getServiceImpact() {
        return 1;
    }


}
