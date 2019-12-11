package com.dexels.logstore;

import java.util.HashMap;
import java.util.Map;

public class LogLevelFilter {
	private static final Map<String, Integer> logLevelMap;
    static {
    	logLevelMap = new HashMap<>();
        logLevelMap.put("NOTICE", 0);
        logLevelMap.put("DEBUG", 1);
        logLevelMap.put("INFO", 2);
        logLevelMap.put("WARN", 3);
        logLevelMap.put("ERROR", 4);
    }
    
    public static boolean isHigherOrEqual(String loglevel, String thresholdLevel) {
		Integer l1 = logLevelMap.get(loglevel);
		Integer l2 = logLevelMap.get(thresholdLevel);
		
		if (l1 == null || l2 == null) {
			// If we don't recognize the logging level, we accept it
			return true;
		}
		
		return l1 >= l2;
	}
	
}
