package com.dexels.monitor.rackermon;

import java.util.Date;

import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public class StateChange {
    public static final String TOPIC = "monitor/statechange";

    private String checkString;
    private int previousState;
    private int currentState;
    private String objectId;
    private Date ts;

    public StateChange(ServerCheckResult oldCheckResult, ServerCheckResult newCheckResult, String objectId) {
        super();
        this.checkString = oldCheckResult.getCheckName();
        this.ts = new Date();
        this.previousState = oldCheckResult.getAlertStatus();
        this.currentState = newCheckResult.getAlertStatus();
        this.objectId = objectId;
    }

    public int getPreviousState() {
        return previousState;
    }

    public int getCurrentState() {
        return currentState;
    }

    public String getObjectId() {
        return objectId;
    }

    public Date getDate() {
        return ts;
    }

    public String getCheckString() {
        return checkString;
    }

}
