package com.steve.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author stevexu
  * @since 10/18/18
  */
object BrandStructSteaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BrandStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sparkSession  = SparkSession.builder.config(sc.getConf).getOrCreate()
    import sparkSession.implicits._

    val df = sparkSession
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "127.0.0.1:9093, 127.0.0.1:9094, 127.0.0.1:9095")
        .option("subscribe", "brandstreaming")
        .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]

    df.writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .outputMode("append")
        .option("truncate","false")
        .start()
        .awaitTermination()

  }

}
