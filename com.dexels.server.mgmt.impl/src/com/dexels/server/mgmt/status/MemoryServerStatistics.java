package com.dexels.server.mgmt.status;

import org.osgi.service.component.annotations.Component;

import com.dexels.server.mgmt.api.ServerStatisticsProvider;

@Component(name = "dexels.server.mgmt.statistic.memory", service = { ServerStatisticsProvider.class }, enabled = true)

public class MemoryServerStatistics implements ServerStatisticsProvider {

    @Override
    public String getKey() {
        return "memory";
    }

    @Override
    public String getValue() {
        long max = Runtime.getRuntime().maxMemory();
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        return (total - free) + "/" + max; 
    }

}
