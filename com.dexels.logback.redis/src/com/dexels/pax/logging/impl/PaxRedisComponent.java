package com.dexels.pax.logging.impl;

import java.util.Map;

import org.ops4j.pax.logging.PaxLoggingService;
import org.ops4j.pax.logging.spi.PaxAppender;
import org.ops4j.pax.logging.spi.PaxLoggingEvent;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.redis.client.RedisClient;



@Component(configurationPolicy=ConfigurationPolicy.REQUIRE, name="pax.redis",immediate=true,property={PaxLoggingService.APPENDER_NAME_PROPERTY+"=PaxAppenderTest"})
public class PaxRedisComponent implements PaxAppender {

	private PaxRedisAppender redisAppender;
	private final static org.slf4j.Logger logger = org.slf4j.LoggerFactory
			.getLogger(PaxRedisComponent.class);

	private static final String DEFAULT_KEY = "logstash";
	private static final String DEFAULT_TYPE = "log";
	private RedisClient redisClient = null;
	
	@Activate
	public void activate(Map<String, Object> settings) {

		try {
			System.err.println("Activating redis");
			redisAppender = createRedis(settings);
			logger.info("Redis appender configured");
			System.err.println("Activated redis");
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Deactivate
	public void deactivate() {
	}

	@Reference(unbind="clearRedisClient",policy=ReferencePolicy.DYNAMIC)
	protected void setRedisClient(RedisClient redis) {
		this.redisClient  = redis;
	}
	
	protected void clearRedisClient(RedisClient redis) {
		this.redisClient = null;
	}
	
//	sourcePath=my source path
//	type=some type
//	tags=aap,noot,mies
//	port=6379
//	key=logstash
//	level=info
//	mdc = true
//	location = true
//	name = 
//	console=true	
	
	private PaxRedisAppender createRedis(Map<String, Object> settings) {
		
		Map<String,String> env = System.getenv();

		redisAppender = new PaxRedisAppender(redisClient);
		String sourcePath = (String) settings.get("sourcePath");
		String sourceHost = (String) settings.get("sourceHost");
		String sourceInstance = getenv(env, "CONTAINERNAME");
		String sourceContainer = getenv(env, "CONTAINER");
		String key = (String) settings.get("key");
		String type = (String) settings.get("type");
		String password = (String) settings.get("password");
		String tags = (String) settings.get("tags");
		
		redisAppender.setMdc((Boolean) parseSetting(settings, "mdc", Boolean.class, Boolean.TRUE));
		redisAppender.setLocation((Boolean) parseSetting(settings, "location", Boolean.class,Boolean.TRUE));
//		Object deDotKeys = settings.get("dedot");
		redisAppender.setDeDotKeys((Boolean) parseSetting(settings, "dedot", Boolean.class, Boolean.TRUE));

		if (sourcePath != null) {
			redisAppender.setSourcePath(sourcePath);
		}
		if (sourceHost != null) {
			redisAppender.setSourceHost(sourceHost);
		}
		if (sourceInstance != null) {
			redisAppender.setSourceInstance(sourceInstance);
		}
		if (sourceContainer != null) {
			redisAppender.setSourceContainer(sourceContainer);
		}

		if (key != null) {
			redisAppender.setKey(key);
		} else {
			redisAppender.setKey(DEFAULT_KEY);			
		}
		if (type != null) {
			redisAppender.setType(type);
		} else {
			redisAppender.setType(DEFAULT_TYPE);
		}
		if (password != null) {
			redisAppender.setPassword(password);
		}
		if (tags != null) {
			redisAppender.setTags(tags);
		}

		return redisAppender;
	}
	
	private final String getenv(Map<String, String> env, String key) {
		String result = env.get(key);
		if(result!=null) {
			return result;
		}
		return System.getProperty(key);
	}

	
	private final <T> Object parseSetting(Map<String, Object> settings, String key, Class<T> type, Object defaultValue) {
		Object value = settings.get(key);
		if(value==null) {
			return defaultValue;
		}
		if(type.isInstance(value)) {
			return type.cast(value);
		}
		// assume string then
		String valueString = (String)value;
		if(type.isAssignableFrom(Boolean.class)) {
			return  new Boolean( Boolean.parseBoolean(valueString));
		}
		if(type.isAssignableFrom(Integer.class)) {
			return  new Integer( Integer.parseInt(valueString));
		}
		throw new IllegalArgumentException("Can't parse value for key: "+key+" value: "+valueString+" requested type: "+type.getName());
	}

	@Override
	public void doAppend(PaxLoggingEvent event) {
		if(redisAppender!=null) {
			redisAppender.doAppend(event);
		} else {
			System.err.println("Dropped log event :"+event.getMessage());
		}
	}
}
