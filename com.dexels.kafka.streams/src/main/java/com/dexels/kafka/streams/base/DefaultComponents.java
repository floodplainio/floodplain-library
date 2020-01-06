package com.dexels.kafka.streams.base;

import java.util.Dictionary;
import java.util.Hashtable;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.replication.transformer.api.MessageTransformer;

@Component(name="dexels.default.components",configurationPolicy=ConfigurationPolicy.IGNORE)
public class DefaultComponents {

	private MessageTransformer identity = (params,msg)->msg;
	
	@Activate
	public void activate(BundleContext context) {
		if("true".equals(System.getenv("DEFAULT_COMPONENTS"))) {
			Dictionary<String, Object> prop = new Hashtable<>();
			prop.put("name","normalizeteamname");
			context.registerService(MessageTransformer.class, identity, prop);
			Dictionary<String, Object> prop2 = new Hashtable<>();
			prop2.put("name","teamorder");
			context.registerService(MessageTransformer.class, identity, prop2);
		}
	}
}
