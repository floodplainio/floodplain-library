package com.dexels.monitor.rackermon;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.monitor.rackermon.checks.api.ServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;
import com.dexels.monitor.rackermon.checks.impl.ServerCheckFactory;
import com.dexels.monitor.rackermon.persistence.Persistence;

@Component(name = "dexels.monitor.cluster", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, service = {
        Cluster.class })
public class Cluster {
    private final static Logger logger = LoggerFactory.getLogger(Cluster.class);
    private Map<String, Object> clusterSettings;
    private Set<Server> servers = new HashSet<>();
    private String name;
    private String type;
    private String myGroup;
    private Integer priority;
    private Integer groupPriority;
    private Set<Deployment> deployments = new HashSet<>();
    
    private ServerCheck checkInFailedCondition = null;

	private Persistence persistence;

    @Activate
    public void activate(Map<String, Object> settings) {
        try {
			clusterSettings = settings;
			
			this.name = (String) settings.get("name");
			this.type = (String) settings.get("type");
			this.myGroup = (String) settings.get("group");
			
			String prio = (String) settings.get("priority");
			if (prio != null) {
			    this.priority = Integer.valueOf(prio);
			}
			
			// Set checks for servers that were added before I had a name...
			for (Server s : getServers()) {
			    if (s.getClusterName().equals(name)) {
			        s.setClusterSettings(clusterSettings);
			        s.setChecks(ServerCheckFactory.getChecks(type));
			    }
			}
		} catch (Throwable e) {
			logger.error("Error: ", e);
		}
       
    }
    
    @Reference(policy = ReferencePolicy.DYNAMIC, name = "Persistence", unbind = "clearPersistence")
    public void setPersistence(Persistence p) {
        this.persistence = p;
    }

    public void clearPersistence(Persistence p) {
        this.persistence = null;
    }
   
    public Set<Server> getServers() {
        Set<Server> result = new HashSet<Server>();
        Server[] myServers = (Server[]) servers.toArray(new Server[servers.size()]);
        for (Server c : myServers) {
            final String clusterName = c.getClusterName();
			if (clusterName.equals(name)) {
                result.add(c);
                c.setClusterPriority(getPriority());
            }
        }
        return result;
    }

    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "removeServer", cardinality = ReferenceCardinality.MULTIPLE)
    public void addServer(Server server) {
        try {
			if (server.getClusterName().equals(name)) {
			    server.setClusterSettings(clusterSettings);
			    if(type!=null) {
				    server.setChecks(ServerCheckFactory.getChecks(type));
			    }
			}
		} catch (Throwable e) {
			logger.error(">Error: ", e);
		}
        servers.add(server);
    }

    public void removeServer(Server server) {
        servers.remove(server);
    }
    

    public String getName() {
        return name;
    }

    public String getGroupName() {
        if (myGroup == null) {
            logger.info("I don't have a group? Where do I belong?? {}",name );
            return "unknown";
        }
        return myGroup;
    }

    public Integer getPriority() {
        if (priority == null) {
            return groupPriority;
        }
        return priority;
    }
    
    

    public void setGroupPriority(Integer groupPriority) {
        this.groupPriority = groupPriority;
    }

    public void runChecks() {
    	
    	Set<Server> servers = new HashSet<Server>(getServers());
        for (final Server s : servers ) {
            s.runChecks();
        }
        
        ServerCheck failed = getImpactingServerCheckFailure();
        if (failed != null) {
            
        	if (checkInFailedCondition == null) {
        		// We have a new failed check
        		persistence.storeClusterAlert(this.getName(), failed);
        	} else if (!failed.equals(checkInFailedCondition))  {
        	    persistence.storeClusterClearedAlert(this.getName(), checkInFailedCondition);
        	    persistence.storeClusterAlert(this.getName(), failed);
        	}
 
        } else if (checkInFailedCondition != null) {
        	// The problem we previously had is now resolved
    		persistence.storeClusterClearedAlert(this.getName(), checkInFailedCondition);
        } else {
        	// Nothing to report - failed = null and no previous failed check
        }
        checkInFailedCondition = failed;
    }
    
    public Map<ServerCheck, ServerCheckResult> getServerCheckResultFailures() {
        Map<ServerCheck, ServerCheckResult> failures = new HashMap<>();
        HashSet<Server> servers = new HashSet<Server>(getServers());

        for (Server s : servers) {
            ServerCheck lastCheck = null;
            for (ServerCheckResult checkResult : s.getCheckResults()) {
                if (checkResult.getAlertStatus() > 0 ) {
                    // We only want 1 alert per server, namely the one with the "highest" service impact 
                    ServerCheck newCheck = s.getCheck(checkResult.getCheckName());
                    if (lastCheck == null || newCheck.getServiceImpact() < lastCheck.getServiceImpact() ) {
                        if (lastCheck != null) failures.remove(lastCheck);
                        failures.put(newCheck, checkResult);
                        lastCheck = newCheck;
                    }
                }
            }
        }

        return failures;
        
    }
    
    
    public Set<ServerCheck> getServerCheckFailures() {
        Set<ServerCheck> failures = new TreeSet<ServerCheck>(ServerCheckComparator);

        for (Server s : getServers()) {
            SortedSet<ServerCheck> serverfailures = new TreeSet<ServerCheck>(ServerCheckComparator);
            for (ServerCheckResult checkResult : s.getCheckResults()) {
                if (checkResult.getAlertStatus() > 0 ) {
                    serverfailures.add(s.getCheck(checkResult.getCheckName()));
                }
            }
          
            // Add the most impacting failure of this server to the global failures
            if (serverfailures.size() > 0) {
                failures.add(serverfailures.first());
            }
          
        }

        return failures;
        
    }
    
	public ServerCheck getImpactingServerCheckFailure() {
	    if (getServers().size() == 0) {
	        logger.debug("No servers for {}", this.getName());
	        return null;
	    }
	    Server aServer = getServers().iterator().next();
		int serverCount = getServers().size();
		Map<String, Integer> failureMap = getFailureCountPerServerCheck();
		if (failureMap.keySet().size() == 0) {
			// nothing is wrong
			return null;
		}

		// Going to sort the ServerChecks on their serviceImpact. A high impact service is thus checked first
		SortedSet<ServerCheck> keys = new TreeSet<ServerCheck>(ServerCheckComparator);
		for (String checkName : failureMap.keySet()) {
		    keys.add(aServer.getCheck(checkName));
		}


		for (ServerCheck c : keys) {
			if (c.getClusterServiceFailureType() == ServerCheck.CLUSTER_FAILURE_ONE_SERVICE && failureMap.get(c.getName()) > 0) {
				return c;
			}
			if (c.getClusterServiceFailureType() == ServerCheck.CLUSTER_FAILURE_ALL_SERVICES
					&& failureMap.get(c.getName()) == serverCount) {
				return c;
			}
		}

		// No failures met the conditions
		return null;
	}
    
	private Map<String, Integer> getFailureCountPerServerCheck() {
		Map<String, Integer> resultMap = new HashMap<>();
		Set<Server> servers = new HashSet<Server>(getServers());
		
		for (Server s : servers) {
			if (s.getStatus() == ServerCheckResult.STATUS_OK) {
				continue;
			}
			for (ServerCheckResult checkResult : s.getCheckResults()) {
				if (checkResult.getAlertStatus() > 0 ) {
					// Something is wrong - lets see if this impacts us
				    String checkName = checkResult.getCheckName();
				    ServerCheck check = s.getCheck(checkName);
                    if (checkResult.getAlertStatus() == ServerCheckResult.STATUS_UNKNOWN && !check.unknownIsFailed()) {
				        // Check status is unknown, but unknown does not count as a failure for this check - ignore
				        continue;
				    }
					
					if (check.getClusterServiceFailureType() != ServerCheck.CLUSTER_FAILURE_NONE ) {
						Integer count = resultMap.get(check.getName());
						if (count == null) {
							count = 0;
						}
						resultMap.put(check.getName(), ++count);
					}
				}
			}
		}
		return resultMap;
	}

    public int getStatus() {
        int myStatus = ServerCheckResult.STATUS_OK;
        
        // Use Doubles for division later
        Double countWarning = 0.0;
        Double countError = 0.0;
        Double countTotal = Double.valueOf(getServers().size());
        
        for (Server s : getServers()) {
            if (s.getStatus() > 0) {
                if (s.getStatus() < ServerCheckResult.STATUS_ERROR) {
                    countWarning++;
                } else {
                    countError++;
                }
            }
        }
        
        // Some fuzzy logic since we don't have to panic when one member is ill
        if (countTotal > 0) {
            if (countError == 0 && countWarning > 0) { 
                myStatus = ServerCheckResult.STATUS_WARNING;
            } else if (countError > 0 ) {
            	// When we have members that are in Error state, we only go to Error 
            	// when this impact more than a third of our cluster members. Otherwise
            	// we go to Warning.
                if (countWarning > 0 || (countError/countTotal) > 0.35) {
                    myStatus = ServerCheckResult.STATUS_ERROR;
                } else {
                    myStatus = ServerCheckResult.STATUS_WARNING; 
                }
            }
        }

        return myStatus;
    }
    
    
    
	Comparator<ServerCheck> ServerCheckComparator = new Comparator<ServerCheck>() {
    	  public int compare(ServerCheck o1, ServerCheck o2) {
    	     return o1.getServiceImpact().compareTo(o2.getServiceImpact());
    	  }
    };

    public Set<Deployment> getDeployments() {
        Set<Deployment> result = new HashSet<Deployment>();
        for (Deployment c : deployments) {
            if (c.getGroupName().equals(myGroup)) {
                result.add(c);
            }
        }
        return result;
    }

    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "removeDeployment", cardinality = ReferenceCardinality.MULTIPLE)
    public void addDeployment(Deployment c) {
        deployments.add(c);
    }

    public void removeDeployment(Deployment c) {
        deployments.remove(c);
    }

}
