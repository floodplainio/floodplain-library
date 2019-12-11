package com.dexels.monitor.rackermon.checks.impl;

import java.util.Map;
import java.util.Set;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.monitor.rackermon.checks.api.ServerCheck;

@Component(name="dexels.monitor.customcheck", configurationPolicy=ConfigurationPolicy.REQUIRE)
public class CustomStatusCheck {
	
//	name=officialportal
//			type=http-js
//			contentToCheck=<input type="text" class="v-textfield v-textfield-tipi-property-direction-in tipi-property-direction-in v-textfield-tipi-property-type-string tipi-property-type-string" value=""/>
//			runIntervalInSeconds=300
//			minimumFailedCheckCount=2
//

	public void activate(Map<String,Object> settings) {
		String type = (String) settings.get("type");
		Set<ServerCheck> res = ServerCheckFactory.getChecks(type);
		
	}
}
