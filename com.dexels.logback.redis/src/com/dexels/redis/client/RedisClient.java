package com.dexels.redis.client;

import org.osgi.annotation.versioning.ProviderType;

import redis.clients.jedis.JedisPool;

@ProviderType
public interface RedisClient {
	public String getHost();
	public int getPort();
	public JedisPool getPool();
	public void rebuild();
}
