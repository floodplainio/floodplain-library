package com.dexels.monitor.rackermon.checks.api;

import com.dexels.monitor.rackermon.Server;

public abstract class AbstractHttpCheck  extends AbstractServerCheck  {
    private static final long serialVersionUID = -1750461498521625359L;

    @Override
    public void setServer(Server server){
        super.setServer(server);
        
        // We retrieve some optional settings
        Object minimumFailedCheckCount = server.getClusterSetting("minimumFailedCheckCount");
        if (minimumFailedCheckCount != null) {
            
            this.minimumFailedCheckCount =  Integer.valueOf((String) minimumFailedCheckCount) ;
        }
        
        Object runIntervalInSeconds = server.getClusterSetting("runIntervalInSeconds");
        if (runIntervalInSeconds != null) {
            this.runIntervalInSeconds = Integer.valueOf((String) runIntervalInSeconds) ;
        }
        
        Object contentToCheck =  server.getClusterSetting("contentToCheck");
        if (contentToCheck != null) {
            this.contentToCheck = (String) contentToCheck;
        }
        
    }
    
    @Override
    public int runIntervalInSeconds() {
        return runIntervalInSeconds;
    }

    
    @Override
    public int getMinimumFailedCheckCount() {
        return minimumFailedCheckCount;
    }
}
