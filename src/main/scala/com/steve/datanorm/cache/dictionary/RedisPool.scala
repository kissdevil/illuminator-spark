package com.steve.datanorm.cache.dictionary


import com.steve.datanorm.GlobalContext
import redis.clients.jedis.{JedisPoolConfig, JedisSentinelPool}
import com.typesafe.scalalogging.Logger
import RedisPool.pool
import com.steve.datanorm.cache.dictionary.entity.RedisCategoryBrand

/**
  * @author stevexu
  * @Since 11/4/17
  */
object RedisRepository {

  val map = scala.collection.mutable.HashMap.empty[String, RedisCategoryBrand]

  def getCategoryBrand(category: String): RedisCategoryBrand = {
    map.get(category) match {
      case Some(s) => {
        return s
      }
      case None => {
        val jedis = pool.getResource()
        try {
          val catBrand = jedis.get("cat:" + category)
          if (catBrand == null) {
            map += (category -> new RedisCategoryBrand(-1, "empty", null))
            return null
          }
          else {
            val objectMapper = GlobalContext.objectMapper
            val catbrand = objectMapper.readValue[RedisCategoryBrand](catBrand)
            map += (category -> catbrand)
            return catbrand
          }
        }
        catch {
          case ex: Exception => println("getCategory brand exception,code:" + category, ex)
        }
        finally {
          jedis.close()
        }
      }
    }
    return null
  }

  def getCategoryBrand(categoryCodes: Array[String]): Array[RedisCategoryBrand] = {
    categoryCodes.map(getCategoryBrand(_))
  }

}


object RedisPool {
  val config = GlobalContext.config
  val poolConfig: JedisPoolConfig = new JedisPoolConfig
  poolConfig.setMaxTotal(config.getInt("brand_norm_app.redis.pool.maxTotal"))
  poolConfig.setMaxIdle(config.getInt("brand_norm_app.redis.pool.maxIdle"))
  poolConfig.setMinIdle(config.getInt("brand_norm_app.redis.pool.minIdle"))
  poolConfig.setMaxWaitMillis(config.getInt("brand_norm_app.redis.pool.maxWait"))
  poolConfig.setTestOnBorrow(true)
  val pool = new JedisSentinelPool(config.getString("brand_norm_app.redis.sentinelpool"), new java.util.HashSet[String](config.getStringList("brand_norm_app.redis.sentinel")), poolConfig, config.getInt("brand_norm_app.redis.timeout"))
}







