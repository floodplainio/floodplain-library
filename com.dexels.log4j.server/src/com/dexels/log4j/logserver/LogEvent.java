package com.dexels.log4j.logserver;

import java.util.Date;
import java.util.Map;

public class LogEvent {

    private String categoryName;
    private String fqnOfCategoryClass;
    private String message;
    private String threadName;
    private Throwable throwable;
    private String level;
    private Date timeStamp;
    private String session;
    private Map<String, String> mdc;

    private Map<String, Object> location;

    public LogEvent() {

    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getFqnOfCategoryClass() {
        return fqnOfCategoryClass;
    }

    public void setFqnOfCategoryClass(String fqnOfCategoryClass) {
        this.fqnOfCategoryClass = fqnOfCategoryClass;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Map<String, Object> getLocation() {
        return location;
    }

    public void setLocation(Map<String, Object> location) {
        this.location = location;
    }

    public String getClassName() {
        if (location == null)
            return null;

        return (String) location.get("className");
    }

    public String getFileName() {
        if (location == null)
            return null;

        return (String) location.get("fileName");
    }

    public Integer getLineNumber() {
        if (location == null)
            return null;

        return (Integer) location.get("lineNumber");
    }

    public String getMethodName() {
        if (location == null)
            return null;

        return (String) location.get("methodName");
    }

    public Map<String, String> getMdc() {
        return mdc;
    }

    public void setMdc(Map<String, String> mdc) {
        this.mdc = mdc;
    }

}