package com.dexels.redis.client.impl;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;

import com.dexels.redis.client.RedisClient;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

@Component(name="dexels.redis.client", configurationPolicy=ConfigurationPolicy.REQUIRE,immediate=true)
public class RedisClientImpl implements RedisClient {

	private String host;
	private int port;

    int timeout = Protocol.DEFAULT_TIMEOUT;
    String password = null;
    int database = Protocol.DEFAULT_DATABASE;
	private JedisPool pool;

	
	@Activate
	public void activate(Map<String,Object> settings) {
		this.host = (String) settings.get("host");
		Object portObject = settings.get("port");
		if(portObject!=null) {
			if(portObject instanceof Integer) {
				Integer port = (Integer) portObject;
				this.port = port;
			}
			if(portObject instanceof String) {
				int port = Integer.parseInt((String) portObject);
				this.port = port;
			}
		}
        createPool();

	}

	private void createPool() {
		System.out.println("Connecting redis client to host: " + host + " port: " + port);
        pool = new JedisPool(new GenericObjectPoolConfig(), host, port, timeout, password, database);
	}

    @Deactivate
    public void deactivate() {
        pool.destroy();
        pool = null;
    }

	
	@Override
	public String getHost() {
		return host;
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public JedisPool getPool() {
		return pool;
	}

	@Override
	public void rebuild() {
		if(pool!=null && !pool.isClosed()) {
			pool.close();
		}
		createPool();
	}

}
