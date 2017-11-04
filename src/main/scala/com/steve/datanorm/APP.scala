package com.steve.datanorm

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait App {

  val session = SparkSession.builder().appName("BrandNorm").master("local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("spark.cassandra.connection.port", "9042")
      .enableHiveSupport()
      .getOrCreate();

}
