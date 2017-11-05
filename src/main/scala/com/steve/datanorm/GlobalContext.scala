package com.steve.datanorm

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
  * @author stevexu
  * @Since 11/4/17
  */
object GlobalContext {

  val config = ConfigFactory.load()

  val session = SparkSession.builder().appName("BrandNorm").master("local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("spark.cassandra.connection.port", "9042")
      .enableHiveSupport()
      .getOrCreate();

  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

}
