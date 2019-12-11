package com.dexels.monitor.rackermon.checks.impl;

import java.util.HashSet;
import java.util.Set;

import com.dexels.monitor.rackermon.checks.api.ServerCheck;

public class ServerCheckFactory {
    public static Set<ServerCheck> getChecks(String type) {
        Set<ServerCheck> checks = new HashSet<>();
        if (type.equals("navajo")) {
            checks.add(new NavajoNormalPoolServerCheck());
            checks.add(new NavajoMemoryServerCheck());
            checks.add(new StatusServletServerCheck());
            checks.add(new NavajoRequestsServerCheck());
        } else if (type.equals("navajobirt")) {
            checks.add(new NavajoBirtServerCheck());
        } else if (type.equals("http")) {
           checks.add(new HttpCheck());
        }
        return checks;
    }

}
