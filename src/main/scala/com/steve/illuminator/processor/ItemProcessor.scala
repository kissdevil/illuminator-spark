package com.steve.illuminator.processor

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author stevexu
  * @Since 10/21/17
  */
object ItemProcessor {

  def process(sc: SparkContext): Unit = {
    val rdd = sc.cassandraTable("buyboxtest", "vendor_items")
    rdd.cache
    println(rdd.count())
  }

}
