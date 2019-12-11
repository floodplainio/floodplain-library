package com.dexels.logback.redis;

import java.util.Date;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.ops4j.pax.logging.PaxLogger;
import org.ops4j.pax.logging.spi.PaxLevel;
import org.ops4j.pax.logging.spi.PaxLocationInfo;
import org.ops4j.pax.logging.spi.PaxLoggingEvent;

import com.dexels.pax.logging.impl.PaxRedisAppender;
import com.dexels.redis.client.RedisClient;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;


public class TestLocalRedis {

	@Test @Ignore
	public void testConnection() {
		int timeout = Protocol.DEFAULT_TIMEOUT;
		String password = null;
		int database = Protocol.DEFAULT_DATABASE;

		
		JedisPool pool = new JedisPool(new GenericObjectPoolConfig(), "localhost", 6379,
				timeout, password, database);

		Jedis client = pool.getResource();
		try {
			long len = client.llen("junit");
			
			client.rpush("junit", "Test: "+new Date());
			long after = client.llen("junit");
			Assert.assertEquals(len+1, after);
		} catch (Exception e) {
			e.printStackTrace();
			pool.close();
			pool = null;
			client = null;
		} finally {
			if (client != null) {
				if(pool!=null) {
					pool.close();
				}
			}
		}
	}
	
//	sourcePath=my source path
//			type=some type
//			tags=
//			key=logstash
//			level=info
//			mdc = true
//			location = true
	
	@Test @Ignore
	public void testLocal() {
		PaxRedisAppender pa = new PaxRedisAppender(new RedisClient() {
			
			@Override
			public int getPort() {
				return 6379;
			}
			
			@Override
			public String getHost() {
				return "localhost";
			}

			@Override
			public JedisPool getPool() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void rebuild() {
				// TODO Auto-generated method stub
				
			}
		});
		pa.setKey("logstash");
		pa.setSourcePath("my sourcePath");
		pa.setSourceHost("localhost");
		pa.setTags("tag1,tag2");
		pa.setMdc(true);
		pa.setLocation(true);
		
		pa.doAppend(new PaxLoggingEvent() {
			
			@Override
			public boolean locationInformationExists() {
				return false;
			}
			
			@Override
			public long getTimeStamp() {
				return new Date().getTime();
			}
			
			@Override
			public String[] getThrowableStrRep() {
				return null;
			}
			
			@Override
			public String getThreadName() {
				return Thread.currentThread().getName();
			}
			
			@Override
			public String getRenderedMessage() {
				return "my renderedmessage";
			}
			
			@Override
			public Map<String, Object> getProperties() {
				return null;
			}
			
			@Override
			public String getMessage() {
				return "my message";
			}
			
			@Override
			public String getLoggerName() {
				return "my logger";
			}
			
			@Override
			public PaxLocationInfo getLocationInformation() {
				return null;
			}
			
			@Override
			public PaxLevel getLevel() {
				return new PaxLevel() {
					
					@Override
					public int toInt() {
						return PaxLogger.LEVEL_INFO;
					}
					
					@Override
					public boolean isGreaterOrEqual(PaxLevel p) {
						return PaxLogger.LEVEL_INFO >= p.toInt();
					}
					
					@Override
					public int getSyslogEquivalent() {
						return 2;
					}

					@Override
					public String toString() {
						return "INFO";
					}
				};
			}
			
			@Override
			public String getFQNOfLoggerClass() {
				return TestLocalRedis.class.getName();
			}
		});
	}
}
