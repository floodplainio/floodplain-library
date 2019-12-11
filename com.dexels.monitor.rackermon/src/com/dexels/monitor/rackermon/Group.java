package com.dexels.monitor.rackermon;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

@Component(name = "dexels.monitor.group", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, service = {
        Group.class })
public class Group {
	
	
	private final static Logger logger = LoggerFactory.getLogger(Group.class);

    private Set<Cluster> clusters = new HashSet<>();
    private String name;
    private Integer priority;
    
    @Activate
    public void activate(Map<String, Object> settings) {
        try {
			this.name = (String) settings.get("name");
			String prio = (String) settings.get("priority");
			if (prio != null) {
			    this.priority = Integer.valueOf(prio);
			}
		} catch (Throwable e) {
			logger.error("Error: ", e);
		}
    }
       
    public Set<Cluster> getClusters() {
        Set<Cluster> result = new HashSet<Cluster>();
        Cluster[] myClusters = (Cluster[]) clusters.toArray(new Cluster[clusters.size()]);
        for (Cluster c : myClusters) {
            if (c.getGroupName().equals(name)){
                result.add(c);
                c.setGroupPriority(priority);
            }
        }
        return result;
    }
    
    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "removeCluster", cardinality = ReferenceCardinality.MULTIPLE)
    public void addCluster(Cluster c) {
        clusters.add(c);        
    }
    
    public void removeCluster(Cluster c) {
        clusters.remove(c);
    }
    
    public String getName() {
        return name;
    }
 
    public int getPriority() {
        return priority;
    }

   
    public int getStatus() {
        int myStatus = ServerCheckResult.STATUS_OK;
//        for (Cluster c : getClusters()) {
//            if (c.getStatus() > myStatus) {
//                // One if my clusters is less happy than me. 
//                // That makes me sad...
//                myStatus = c.getStatus();
//            }
//
//        }
        return myStatus;
    }
    

    public void runChecks() {
        HashSet<Cluster> clusters = new HashSet<Cluster>(getClusters());
        for (Cluster member: clusters) {
            member.runChecks();
        }
    }
    
    
}
