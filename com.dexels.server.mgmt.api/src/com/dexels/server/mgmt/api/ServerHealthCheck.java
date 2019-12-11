package com.dexels.server.mgmt.api;

public interface ServerHealthCheck  {
    public boolean isOk();
    public String getDescription();
}