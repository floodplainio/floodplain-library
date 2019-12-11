package com.dexels.monitor.rackermon.persistence;

import java.util.ArrayList;
import java.util.List;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.monitor.rackermon.ClusterAlert;
import com.dexels.monitor.rackermon.StateChange;
import com.dexels.monitor.rackermon.checks.api.ServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

@Component(name = "dexels.monitor.dummypersistence",  configurationPolicy=ConfigurationPolicy.REQUIRE, immediate=true,  service = {
        DummyPersistence.class, Persistence.class}, property={"service.ranking:Integer=-2000"} )
public class DummyPersistence implements Persistence {
   
    
    public DummyPersistence() {
        
    }
    
    @Override
    public void storeStateChange(StateChange s) {        
         
    }

    @Override
    public void storeStatus(String objectId, ServerCheckResult status) {
        
    }

	@Override
	public void storeClusterAlert(String name, ServerCheck failed) {

		
	}

	@Override
	public void storeClusterClearedAlert(String name, ServerCheck checkInFailedCondition) {

		
	}

    @Override
    public List<ClusterAlert> getHistoricClusterAlerts(String name) {
        return new ArrayList<ClusterAlert>();
    }

	@Override
	public String getUptime(String clusterName) {
		return "0/0/0";
	}

}
