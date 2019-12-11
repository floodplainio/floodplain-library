package com.dexels.server.mgmt.status;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.server.mgmt.api.ServerHealthCheck;
import com.dexels.server.mgmt.shutdown.ServerShutdownImpl;

@Component(name = "dexels.server.mgmt.statuscheck.shutdown", service = { ServerHealthCheck.class }, enabled = true)
public class ShutdownServerCheck implements ServerHealthCheck{
    ServerShutdownImpl shutdowninstance;
    
    @Override
    public boolean isOk() {
        if (shutdowninstance == null) {
            return true;
        }
        return shutdowninstance.shutdownInProgress() == false;
    }

    @Override
    public String getDescription() {
        return "shutdown scheduled";
    }
    

    @Reference(unbind="clearShutdownInstance", policy=ReferencePolicy.DYNAMIC)
    public void setShutdownInstance(ServerShutdownImpl i) {
        shutdowninstance = i;
    }

    public void clearShutdownInstance(ServerShutdownImpl i) {
        i = null;
    }

}
