package com.dexels.monitor.rackermon;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import com.dexels.hazelcast.HazelcastService;
import com.dexels.monitor.rackermon.checks.api.ServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;
import com.dexels.monitor.rackermon.persistence.Persistence;

@Component(name = "dexels.monitor.server", immediate = true,configurationPolicy = ConfigurationPolicy.REQUIRE,  service = {
        Server.class })
public class Server  {
    private final static Logger logger = LoggerFactory.getLogger(Server.class);
    private static final long MAX_MAINTENANCE_DURATION_IN_MS = 3 * 60 * 60 * 1000; // 3 hours
    
    private Map<String, Object> clusterSettings;
    
    private Object sync = new Object();
    private String servicePid;
    private String displayName;
    private String hostname;
    private String serverName;
    private String containerName;

    private Integer checkTimeout = 5;
    private String myCluster;
    private Integer priority;
    private Integer clusterPriority;

    private Set<Object> checkResults;
    private Set<ServerCheck> myChecks;
    private Persistence persistence;
    private HazelcastService hazelcastService;
	private Optional<String> ip;
	private Integer defaultPort;


    @Activate
    public void activate(Map<String, Object> settings) {      
        try {
			servicePid = (String) settings.get("service.pid");
			myCluster = (String) settings.get("cluster");
			
			containerName = (String) settings.get("containerName");
			serverName = (String) settings.get("serverName");

			ip = Optional.ofNullable((String) settings.get("ip"));
			defaultPort = (Integer)settings.get("defaultPort");

			displayName =  containerName + "-" + serverName;
			
			if (settings.containsKey("hostname")) {
			    this.hostname = (String) settings.get("hostname");
			}

			String prio = (String) settings.get("priority");
			if (prio != null) {
			    this.priority = Integer.valueOf(prio);
			}
			String checkTimeout = (String) settings.get("checkTimeout");
			if (checkTimeout != null) {
			    this.checkTimeout = Integer.valueOf(checkTimeout);
			}
			String maintenance = (String) settings.get("maintenance");
			if (maintenance != null && maintenance.equalsIgnoreCase("true")) {
			    // Start server as being in maintenance
			    this.setMaintenance(true);
			}
			checkResults = hazelcastService.getSet("CheckResults" + getName());
		} catch (Throwable e) {
			logger.error("Error: ", e);
		}
    }
    
    protected void setClusterSettings(Map<String, Object> settings) {
        this.clusterSettings = settings;
    }
    
    public Object getClusterSetting(String key) {
        return clusterSettings.get(key);
    }

    public void runChecks() {
    	
        for (final ServerCheck check : myChecks) {
            if (check.isTimeToRun()) {
                new Thread() {
                    public void run() {
                        ServerCheckResult checkRes = check.performCheck();
                        processServerCheckResult(checkRes);
                    }
                }.start();
            }
        }
    }

    private void processServerCheckResult(ServerCheckResult newCheckResult) {
        synchronized (sync) {
            for (Object checkObject : checkResults) {
                ServerCheckResult aCheckResult = (ServerCheckResult) checkObject;
                if (aCheckResult.getCheckName().equals(newCheckResult.getCheckName())) {
                    int previousState = aCheckResult.getAlertStatus();
                    if (previousState != newCheckResult.getAlertStatus()) {
                        logger.info("Status change {} to {} for {}!", previousState, newCheckResult.getAlertStatus(), newCheckResult.getCheckName()  );
                        StateChange change = new StateChange(aCheckResult, newCheckResult, getName());
                        persistence.storeStateChange(change);
                        newCheckResult.setStatusSinceDate(new Date());
                    } else {
                        newCheckResult.setStatusSinceDate(aCheckResult.getStatusSinceDate());
                    }
                    checkResults.remove(aCheckResult);
                }
            }
            if(this.persistence!=null) {
                persistence.storeStatus(getName(), newCheckResult);
            } else {
            	logger.warn("Can't persist new state: {}",getName());
            }
            try {
                checkResults.add(newCheckResult);
            } catch (Exception e) {
                logger.error("Exception on adding check result to Hazelcast set: {}", e);
            }

        }
    }

    public List<ServerCheckResult> getCheckResults() {
        List<ServerCheckResult> result = new ArrayList<>();
        synchronized (sync) {
            for (Object o : checkResults) {
                result.add((ServerCheckResult) o);
            }
        }
        return result;
    }

    public int getStatus() {
        int myStatus = ServerCheckResult.STATUS_OK;
        synchronized (this) {
            synchronized (sync) {
                for (Object anObject : checkResults) {
                    ServerCheckResult aCheckResult = (ServerCheckResult) anObject;
                    if (aCheckResult.getStatus() > myStatus)
                        // One if my checks is less happy than me.
                        // That makes me sad...
                        myStatus = aCheckResult.getStatus();
                }
            }
        }
        return myStatus;
    }

    public String getName() {
        return displayName;
    }
    
    public String getServerName() {
        return serverName;
    }
    
    public String getContainerName() {
        return containerName;
    }

    
    /** Return a server-specific hostname. Might be null! */
    public String getMyHostname() {
        return hostname;
    }
    public Optional<String> getEndPoint() {
    	return this.ip.map(e->"http://"+e+":"+defaultPort);
    }

    public String getClusterName() {
        if (myCluster == null) {
            logger.info("I don't have a cluster? Where do I belong?? {}", getName());
            return "unknown";
        }
        return myCluster;
    }
    
    public Integer getCheckTimeout() {
        if (myCluster == null) {
            logger.info("I don't have a cluster? Where do I belong?? {}", getName());
            return 5;
        }
        return checkTimeout;
    }

    public Integer getPriority() {
        if (priority == null) {
            return clusterPriority;
        }
        return priority;
    }
    
    public String getServicePid() {
        return servicePid;
    }

    public void setChecks(Set<ServerCheck> checks) {
        for (ServerCheck check : checks) {
            check.setServer(this);
        }
        this.myChecks = checks;
    }

    @Reference(policy = ReferencePolicy.DYNAMIC, name = "Persistence", unbind = "clearPersistence")
    public void setPersistence(Persistence p) {
        this.persistence = p;
    }

    public void clearPersistence(Persistence p) {
        this.persistence = null;
    }

   
    public void setClusterPriority(Integer clusterPriority) {
        this.clusterPriority = clusterPriority;
    }

    @Reference(name = "HazelcastService", unbind = "clearHazelcastService", policy = ReferencePolicy.DYNAMIC)
    public void setHazelcastService(HazelcastService service) {
        this.hazelcastService = service;
    }

    public void clearHazelcastService(HazelcastService service) {
        this.hazelcastService = null;
    }

    public ServerCheck getCheck(String checkName) {
        for (final ServerCheck check : myChecks) {
            if (check.getName().equals(checkName)) {
                return check;
            }
        }
        return null;
    }

    public void setMaintenance(boolean b) {
        logger.info("Updating maintenance mode: {} for {}", b, this.getName());
        hazelcastService.getMap("maintenancemode").put(this.getName(), b);
        hazelcastService.getMap("maintenancestarttime").put(this.getName(), new Date().getTime());
    }
    
    public boolean getMaintenanceMode() {
        Boolean maintenanceMode = (Boolean) hazelcastService.getMap("maintenancemode").get(this.getName());
        Long maintenanceModeStartTime = (Long) hazelcastService.getMap("maintenancestarttime").get(this.getName());
        
        if (maintenanceMode == null) {
            maintenanceMode = false;
        }
        if (maintenanceMode && maintenanceModeStartTime == null) {
            // This shouldn't really happen?
            maintenanceModeStartTime = new Date().getTime();
            hazelcastService.getMap("maintenancestarttime").put(this.getName(), new Date().getTime());
        }
        if (maintenanceMode && (new Date().getTime() - maintenanceModeStartTime > MAX_MAINTENANCE_DURATION_IN_MS)) {
            logger.warn("Disabling maintenance mode for {} since max time is expired!", this.getName());
            hazelcastService.getMap("maintenancemode").remove(this.getName());
            hazelcastService.getMap("maintenancestarttime").remove(this.getName());
            maintenanceMode = false;
        }
        return maintenanceMode;
    }

}
