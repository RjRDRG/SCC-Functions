package scc.cache;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scc.mgt.AzureProperties;

public class Cache {
	
	private static JedisPool instance;
	
	public synchronized static JedisPool getInstance() {
		if(instance == null) {
			final JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxTotal(128);
			poolConfig.setMaxIdle(128);
			poolConfig.setMinIdle(16);
			poolConfig.setTestOnBorrow(true);
			poolConfig.setTestOnReturn(true);
			poolConfig.setTestWhileIdle(true);
			poolConfig.setNumTestsPerEvictionRun(3);
			poolConfig.setBlockWhenExhausted(true);
			instance = new JedisPool(
					poolConfig,
					AzureProperties.getProperty("REDIS_URL"),
					6380,
					1000,
					AzureProperties.getProperty( "REDIS_KEY"),
					true
			);
		}

		return instance;
	}

}
