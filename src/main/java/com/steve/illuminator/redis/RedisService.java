package com.steve.illuminator.redis;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author stevexu
 * @Since 11/2/17
 */
public class RedisService {

    static JedisSentinelPool pool;

    private static class RedisConfigHolder {
        private static final RedisService INSTANCE = new RedisService();
    }

    public static final RedisService getInstance() {
        return RedisConfigHolder.INSTANCE;
    }

    public JedisSentinelPool getPool() {
        return pool;
    }

    public JedisSentinelPool initJedisSentinelPool(Config config) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getInt("brand_norm_app.redis.pool.maxTotal"));
        poolConfig.setMaxIdle(config.getInt("brand_norm_app.redis.pool.maxIdle"));
        poolConfig.setMinIdle(config.getInt("brand_norm_app.redis.pool.minIdle"));
        poolConfig.setMaxWaitMillis(config.getInt("brand_norm_app.redis.pool.maxWait"));
        poolConfig.setTestOnBorrow(true);
        pool = new JedisSentinelPool(config.getString("brand_norm_app.redis.sentinelpool"),
                                     new HashSet<>(config.getStringList("brand_norm_app.redis.sentinel")),
                                     poolConfig, config.getInt("brand_norm_app.redis.timeout"));
        return pool;
    }

}
