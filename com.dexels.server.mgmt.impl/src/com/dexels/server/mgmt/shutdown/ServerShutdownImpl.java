package com.dexels.server.mgmt.shutdown;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.server.mgmt.api.ServerShutdownCheck;

@Component(name = "dexels.server.mgmt.shutdown",  service = { ServerShutdownImpl.class }, enabled = true)
public class ServerShutdownImpl implements Runnable {
    private static final int INIT_SLEEP_LB = 3000;

    private final static Logger logger = LoggerFactory.getLogger(ServerShutdownImpl.class);
    
    private boolean shutdownInProgress = false;
    private BundleContext bundlecontext;
    private long startedShutdownAt;
    private int timeout;

    private Set<ServerShutdownCheck> checks = new HashSet<>();
    
    @Activate
    public void activate(BundleContext bc, Map<String, Object> settings) {
        this.bundlecontext = bc;
    }

    public boolean shutdownInProgress() {
        return shutdownInProgress;
    }

    public void cancelShutdownInProgress() {
        logger.info("Attempting to cancel shutdown");
        shutdownInProgress = false;
    }

    public void setTimeout(int timeout) {
        logger.info("Setting timeout of {} seconds for shutdown", timeout);
        this.timeout = timeout;
    }

    public void start() {
        synchronized(this) {
            if (shutdownInProgress) {
               logger.warn("Cannot reschedule existing shutdown");
               return;
            }
            new Thread(this).start();
        }
    }

    @Override
    public void run() {
        shutdownInProgress = true;

        logger.warn("OSGi shutdown scheduled!");
        boolean expired = false;
        
        // We start by sleeping a few seconds, to give the loadbalancer time to 
        // see that we are about to shut down
        try {
            Thread.sleep(INIT_SLEEP_LB); 
        } catch (InterruptedException e) {
        }
            
        
        startedShutdownAt = new Date().getTime();
        // At this point we can assume no new requests will be coming in

        while (shutdownInProgress) {
            if (timeout > 0) {
                expired = (new Date().getTime() - startedShutdownAt) > (timeout * 1000);
            }

            boolean allowShutdown = true;
            if (expired) {
                logger.warn("Shutdown timeout of {}s expired - commencing shutdown", timeout);
            } else {
                for (ServerShutdownCheck c : checks) {
                    allowShutdown = allowShutdown & c.allowShutdown();
                }
            }            
            
            if (expired || allowShutdown) {
                try {
                    startSystemShutdown();
                } catch (Exception e) {
                    logger.error("Exception on performing system shutdown: ", e);
                }
                return;
            }

        }

        logger.info("Finishing Shutdown thread!");
    }

    private void startSystemShutdown() {
        logger.warn("Actual shutdown imminent");

        // Last chance to stop
        if (!shutdownInProgress) {
            logger.warn("Shutdown !");
            return;
        }
        logger.warn("Going to stop System Bundle");
        try {
            bundlecontext.getBundle(0).stop();
        } catch (BundleException e) {
            logger.error("Stopping bundle 0 ( system-bundle) gave a BundleException: ", e);
        }
        logger.warn("Shutdown completed");

    }

    
    @Reference(cardinality=ReferenceCardinality.MULTIPLE,unbind="removeShutdownCheck",policy=ReferencePolicy.DYNAMIC)
    public void addShutdownCheck(ServerShutdownCheck c) {
        checks.add(c);
    }
    
    public void removeShutdownCheck(ServerShutdownCheck c) {
        checks.remove(c);
    }
}

