package com.dexels.hazelcast.mgmt;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.HazelcastService;
import com.dexels.server.mgmt.api.ServerShutdownCheck;

@Component(name="dexels.hazelcast.mgmt.shutdown",immediate=true,service={ServerShutdownCheck.class})
public class HazelcastShutdownCheck implements ServerShutdownCheck {
    private static final Logger logger = LoggerFactory.getLogger(HazelcastShutdownCheck.class);

    private HazelcastService hazelcastService;

    @Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearHazelcastService")
    public void setHazelcastService(HazelcastService hazelcastService) {
        this.hazelcastService = hazelcastService;
    }

    public void clearHazelcastService(HazelcastService hzService) {
        this.hazelcastService = null;
    }

    @Override
    public boolean allowShutdown() {
        if (hazelcastService == null) {
            logger.warn("No hazelcast service found - allowing shutdown");
            return true;
        }
        
        boolean clusterSafe = hazelcastService.getHazelcastInstance().getPartitionService().isClusterSafe();
        boolean localSafe = hazelcastService.getHazelcastInstance().getPartitionService().isLocalMemberSafe();
        return clusterSafe && localSafe;

    }

}
