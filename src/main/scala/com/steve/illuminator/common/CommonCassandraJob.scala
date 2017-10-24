package com.steve.illuminator.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author stevexu
  * @Since 10/21/17
  */
trait CommonCassandraJob {

  val conf = new SparkConf(true).setAppName("SteveTestDataStax")
      /*.setMaster("yarn")*/
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.connection.port", "9042")

  val sc = new SparkContext(conf)

  val ss = SparkSession.builder().appName("SteveTestDataStax")/*.master("yarn")*/
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate();

  def init(args: Array[String]): Unit = {

  }

}
