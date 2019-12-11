package com.dexels.log4j.httpappender;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HttpLogAppender extends AppenderSkeleton  implements Runnable {
    private static final int MAX_LOG_BACKLOG = 1000;
    private static final long MAX_TIME_DIFF = 30 * 1000; // 30 seconds

    private LogEventLayout layout;
    private LogShipper shipper;
    private List<LoggingEvent> logEntries;
    private ObjectMapper mapper;
    private ScheduledExecutorService exec;


    public HttpLogAppender() {
        System.out.println("Activated HttpLogAppender");
        layout = new LogEventLayout();
        shipper = new LogShipper();
        logEntries = new ArrayList<>();
        mapper = new ObjectMapper();

        shipper.setRemoteLoggerURL(getSystemProperty("jnlp.loggerurl"));
        layout.setApplication(getSystemProperty("jnlp.application"));
        layout.setVmArguments(getOSVMArguments());
        
        // Activating the shipper should have resulted in knowing the timediff
        if (Math.abs(shipper.getTimeDiff()) > MAX_TIME_DIFF) {
            layout.setTimeCorrection(shipper.getTimeDiff());
        }

        exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Thread(this), 0, 5, TimeUnit.SECONDS);
    }

    private String getSystemProperty(String key) {
        try {
            final String env = System.getenv(key);
            if (env != null) {
                return env;
            }
        } catch (Throwable t) {
        }
        
        try {
            return System.getProperty(key);
        } catch (Throwable t) {
            
        }
        return null;

    }

    @Override
    public void close() {
        layout = null;
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    protected void append(LoggingEvent event) {
        if (MDC.getCopyOfContextMap() != null) {
            // Going to set all MDC values as properties
            for (String mdcKey : MDC.getCopyOfContextMap().keySet()) {
                event.setProperty(mdcKey, MDC.get(mdcKey));
            }
        }
        synchronized (this) {
            logEntries.add(event);
        }
    }

    private Map<String, String> getOSVMArguments() {
        Map<String, String> res = new HashMap<>();
        
        try {
            res.put("java.version", getSystemProperty("java.version"));
            res.put("os", getSystemProperty("os.name") + " " + getSystemProperty("os.version"));
            res.put("cpucores" , String.valueOf(Runtime.getRuntime().availableProcessors()));
            
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            Object memattribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
            res.put("memory", memattribute.toString());
            
            String realArch = System.getProperty("os.arch").endsWith("64") ? "64" : "32";

            if (System.getProperty("os.name").startsWith("Windows")) {
                res.put("cpu", System.getenv("PROCESSOR_IDENTIFIER"));
                
                // WOW64 messes arch up
                String arch = System.getenv("PROCESSOR_ARCHITECTURE");
                String wow64Arch = System.getenv("PROCESSOR_ARCHITEW6432");
                realArch = arch.endsWith("64")
                        || wow64Arch != null && wow64Arch.endsWith("64")
                        ? "64" : "32";
            }
            res.put("arch", realArch);
        }
       
        catch (Exception e) {
            // ignore
        }
        return res;
    }

    @Override
    public void run() {
        if (logEntries.size() < 1) {
            // Nothing to ship!
            return;
        }
        
        try {
            List<String> logs = new ArrayList<>();
            for (LoggingEvent event : logEntries) {
                if (logs.size() > MAX_LOG_BACKLOG) {
                    // stop adding new
                    continue;
                }
                String formatted = layout.format(event);
                if (formatted != null) {
                    logs.add(formatted);
                }
            }
            List<String> logsToShip = new ArrayList<>(logs);
            shipper.ship(mapper.writeValueAsString(logsToShip));
            if (Math.abs(shipper.getTimeDiff()) > MAX_TIME_DIFF) {
                layout.setTimeCorrection(shipper.getTimeDiff());
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            logEntries.clear();
        }
    }

  
}
