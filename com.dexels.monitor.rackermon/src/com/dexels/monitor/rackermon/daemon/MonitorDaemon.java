package com.dexels.monitor.rackermon.daemon;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.HazelcastService;
import com.dexels.monitor.rackermon.Group;

@Component(name = "dexels.monitor.daemon", immediate = true)
public class MonitorDaemon implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(MonitorDaemon.class);

    
    private static final long CHECK_INTERVAL = 10000;
    boolean keepRunning = true;
    private List<Group> myGroups = new ArrayList<>();
    private HazelcastService hazelcastService;

    @Activate
    public void activate(BundleContext context) {
        (new Thread(this)).start();
    }

    @Deactivate
    public void deactivate() {
        keepRunning = false;
    }

    @Override
    public void run() {

        while (keepRunning) {
            if (isOldestMember()) {
            	List<Group> checkGroups = new ArrayList<Group>(myGroups);
                for (Group aGroup : checkGroups) {
                    try {
                        aGroup.runChecks();
                    }catch (ConcurrentModificationException e) {
                        logger.warn("Exception on running checks for {}. But it's not really important", aGroup.getName(), e);
                    } catch (Exception e) {
                        logger.error("Exception on running checks for {}", aGroup.getName(), e);
                    }
                }
            } else {
                logger.debug("Skipping checks since I am not the oldest member...");
            }
            try {
                Thread.sleep(CHECK_INTERVAL);
            } catch (InterruptedException e) {
                keepRunning = false;
            }

        }
        logger.warn("Stopping Monitor Thread! No more checks");
    }

    private boolean isOldestMember() {
        return hazelcastService.oldestMember().equals(hazelcastService.getHazelcastInstance().getCluster().getLocalMember());
    }

    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "removeGroup", cardinality = ReferenceCardinality.MULTIPLE)
    public void addGroup(Group group) {
        myGroups.add(group);
    }

    public void removeGroup(Group group) {
        myGroups.remove(group);
    }
    
    @Reference(name = "HazelcastService", unbind = "clearHazelcastService", policy=ReferencePolicy.DYNAMIC)
    public void setHazelcastService(HazelcastService service) {
        this.hazelcastService = service;
    }

    public void clearHazelcastService(HazelcastService service) {
        this.hazelcastService = null;
    }

}
