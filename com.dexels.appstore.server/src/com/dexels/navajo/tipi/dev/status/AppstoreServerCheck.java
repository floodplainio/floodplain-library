package com.dexels.navajo.tipi.dev.status;

import java.util.HashMap;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.tipi.dev.server.appmanager.impl.RepositoryInstanceWrapper;
import com.dexels.server.mgmt.api.ServerHealthCheck;


@Component(name = "dexels.server.mgmt.statuscheck.appstore", service = { ServerHealthCheck.class }, enabled = true)
public class AppstoreServerCheck implements ServerHealthCheck {
    
    private final Map<String, RepositoryInstanceWrapper> applications = new HashMap<>();

    @Override
    public boolean isOk() {
        if (applications.size() < 1) {
            // We should have at least one RepositoryInstance, right?
            return false;
        }
        for (RepositoryInstanceWrapper wrapped : applications.values()) {
            if (!wrapped.getServerStatusIsOk()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getDescription() {
        String notBuilt = "";
        if (applications.size() < 1) {
            notBuilt = "No applications";
        }
        for (RepositoryInstanceWrapper wrapped : applications.values()) {
            if (!wrapped.getServerStatusIsOk()) {
                notBuilt += wrapped.getRepositoryName() + " ";
            }
        }
        if (notBuilt.equals("")) {
            return "appstore";
        }
        return "Not all applications built: " + notBuilt;
    }
    
    @Reference(cardinality = ReferenceCardinality.MULTIPLE, unbind = "removeRepositoryInstance", policy = ReferencePolicy.DYNAMIC)
    public void addRepositoryInstance(RepositoryInstance a) {
        applications.put(a.getRepositoryName(), new RepositoryInstanceWrapper(a));
    }

    public void removeRepositoryInstance(RepositoryInstance a) {
        applications.remove(a.getRepositoryName());
    }
    


}
