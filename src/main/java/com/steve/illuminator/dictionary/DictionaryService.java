package com.steve.illuminator.dictionary;

import com.google.common.collect.Lists;
import com.steve.illuminator.redis.RedisService;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.Map;

/**
 * @author stevexu
 * @Since 11/2/17
 */
@Slf4j
public class DictionaryService {

    public static Map<String, String> getDictionary(){
        JedisSentinelPool pool = RedisService.getInstance().getPool();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.hgetAll("dictionary");
        } catch (JedisException ex) {
             log.error("exception while finding dictionary",ex);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    public static List<String> getBrandByCategory(Long categoryCode){
        JedisSentinelPool pool = RedisService.getInstance().getPool();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return Lists.newArrayList(jedis.get(String.valueOf(categoryCode)));
        } catch (JedisException ex) {
            log.error("exception while finding dictionary",ex);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }



}
