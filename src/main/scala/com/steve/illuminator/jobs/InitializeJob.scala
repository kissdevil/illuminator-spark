package com.steve.illuminator.jobs

import com.steve.illuminator.common.CommonCassandraJob
import com.steve.illuminator.dictionary.DictionaryService
import com.steve.illuminator.processor.ItemProcessor
import com.steve.illuminator.redis.RedisService
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._


object InitializeJob extends CommonCassandraJob {

  val config = ConfigFactory.load()

  def main(args: Array[String]) {
    println("== Initialize Job ==")

    init(args)

    ItemProcessor.process(sc, ss)
  }

  def init(args: Array[String]): Unit = {
    RedisService.getInstance().initJedisSentinelPool(config);
    println(getDictMap)
  }

  def getDictMap(): Map[String,String] = {
    DictionaryService.getDictionary().toMap
  }



}
