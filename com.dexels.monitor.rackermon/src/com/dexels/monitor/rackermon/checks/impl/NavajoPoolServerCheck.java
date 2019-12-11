package com.dexels.monitor.rackermon.checks.impl;

import com.dexels.monitor.rackermon.checks.api.AbstractServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public abstract class NavajoPoolServerCheck extends AbstractServerCheck {
    private static final long serialVersionUID = 8324950708633960952L;
    private static final int POOL_WARNING_LIMIT_PERCENTAGE = 65;
    private static final int POOL_ERROR_LIMIT_PERCENTAGE = 85;
    
    
    @Override
    
    public int getMinimumFailedCheckCount() {
        return 5;
    }
    
    protected void parsePoolResult(ServerCheckResult s, String response) {
        String[] splitted = response.split("/");
        if (splitted.length < 3) {
            // unexpected result
            s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
            return;
        }
        Double totalsize = Double.valueOf(splitted[1]);
        Double running = Double.valueOf(splitted[0]);
        //int queued = Integer.valueOf(splitted[2]);
        
//        Integer runInt = (new Random()).nextInt(100);
//        running = runInt.doubleValue();
        
        Double percentageFull = (running / totalsize) * 100;
      

        if (percentageFull >= POOL_ERROR_LIMIT_PERCENTAGE) {
            s.setStatus(ServerCheckResult.STATUS_ERROR);
        } else if (percentageFull >= POOL_WARNING_LIMIT_PERCENTAGE) {
            s.setStatus(ServerCheckResult.STATUS_WARNING);
        } else {
            s.setStatus(ServerCheckResult.STATUS_OK);
        }
    }
    
    public static void main (String[] args) {
        
        Double totalsize = 40.0;
        Double running = 20.0;
        //int queued = Integer.valueOf(splitted[2]);
        
        Double percentageFull = (running / totalsize) * 100;

        if (percentageFull >= POOL_ERROR_LIMIT_PERCENTAGE) {
            System.out.println("err");
        } else if (percentageFull >= POOL_WARNING_LIMIT_PERCENTAGE) {
            System.out.println("warn");
        } else {
            System.out.println("OK!");
        }
        
    }

}
