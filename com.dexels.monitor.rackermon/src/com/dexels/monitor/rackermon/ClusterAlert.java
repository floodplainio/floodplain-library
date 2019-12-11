package com.dexels.monitor.rackermon;

import java.util.Date;

public class ClusterAlert {

    private String clusterName;
    private Date alertFrom;
    private Date alertTo;
    private String alertDescription;
    
    public ClusterAlert(String clusterName, String alertDescription, Date alertFrom, Date alertTo) {
        this.clusterName = clusterName;
        this.alertFrom = alertFrom;
        this.alertTo = alertTo;
        this.alertDescription = alertDescription;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Date getAlertFrom() {
        return alertFrom;
    }

    public Date getAlertTo() {
        return alertTo;
    }

    public String getAlertDescription() {
        return alertDescription;
    }
    
    
    
}
