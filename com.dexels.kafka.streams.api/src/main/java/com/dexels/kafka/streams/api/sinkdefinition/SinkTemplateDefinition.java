package com.dexels.kafka.streams.api.sinkdefinition;

import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

@Component(name="dexels.streams.sink",configurationPolicy=ConfigurationPolicy.REQUIRE,service=SinkTemplateDefinition.class)
public class SinkTemplateDefinition {
	
	private String name;
	private Map<String, Object> settings;

	public String getName() {
		return name;
	}

	public Map<String, Object> getSettings() {
		return this.settings;
	}
	@Activate
	public void activate(Map<String,Object> settings) {
		name = (String) settings.get("name");
		this.settings = settings;
	}
}
