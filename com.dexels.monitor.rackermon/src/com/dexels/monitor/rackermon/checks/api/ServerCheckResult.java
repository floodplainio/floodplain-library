package com.dexels.monitor.rackermon.checks.api;

import java.io.Serializable;
import java.util.Date;

public class ServerCheckResult implements Serializable {
    private static final long serialVersionUID = 3100792111671643757L;
    
    public final static int STATUS_OK = 0;
    public final static int STATUS_WARNING = 1;
    public final static int STATUS_ERROR = 2;
    public final static int STATUS_UNKNOWN = 3;
    
    private int status;
    private String checkName;
    /* AlertStatus differs from the normal status, since 
     * it's not always desirable that a change in status immediately
     * triggers an alert. The ServerCheck determines when a status 
     * becomes an alertStatus
     */
    private int alertStatus;

    private String rawResult;
    private Date statusSinceDate = new Date();
    
    public ServerCheckResult(ServerCheck check) {
    	this.status = STATUS_OK;
    	this.checkName = check.getName();
    }

    public ServerCheckResult(ServerCheckResult anotherStatus) {
        this.status = anotherStatus.getStatus();
        this.checkName = anotherStatus.getCheckName();
    }

    public ServerCheckResult setStatus(int newStatus) {
        this.status = newStatus;
        return this;
    }

    public int getStatus() {
        return status;
    }
   
    public int getAlertStatus() {
        return alertStatus;
    }

    public void setAlertStatus(int alertStatus) {
        this.alertStatus = alertStatus;
    }


	public String getRawResult() {
        return rawResult;
    }

    public void setRawResult(String rawResult) {
        this.rawResult = rawResult;
    }
    
    public Date getStatusSinceDate() {
        return statusSinceDate;
    }

    public void setStatusSinceDate(Date sinceDate) {
        this.statusSinceDate = sinceDate;
    }
    
    public String getCheckName() {
        return checkName;
    }

}
