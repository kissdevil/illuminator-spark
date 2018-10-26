package com.steve.streaming

import java.util.Arrays

import com.steve.kafka.serialize.ReconciledMessageDeSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.streaming.kafka010._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions


/**
  * @author stevexu
  * @since 10/15/18
  */
object BrandStreaming {

  private val logger: Logger = LoggerFactory.getLogger(BrandStreaming.getClass)

  def main(args: Array[String]): Unit = {
    val kafkaParams = collection.mutable.Map[String, Object]()
    kafkaParams += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095")
    kafkaParams += (ConsumerConfig.GROUP_ID_CONFIG -> "brandstreaminggroup")
    kafkaParams += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
    //kafkaParams += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "none")
    //kafkaParams += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "1000")
    kafkaParams += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[com.steve.deserializer.ReconciledMessageDeserializer])

    val conf = new SparkConf().setAppName("BrandStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(30))

    val topic = "brandstreaming"

    val streaming =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, ReconciledMessage](
          Arrays.asList(topic),
          JavaConversions.mapAsJavaMap(kafkaParams)
        )

      )

    streaming.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("rdd has " + rdd.getNumPartitions + " partitions")
        rdd.glom().foreach(a => println("*** partition size = " + a.size))
      }
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val sparkContext = rdd.sparkContext
      val sparkSession = SparkSession.builder.config(sparkContext.getConf).getOrCreate()
      import sparkSession.implicits._

      val converted = rdd.map(rdd => rdd.value())
      val df = converted.toDF()
      logger.info("ds has " + df.rdd.getNumPartitions + " partitions")
      logger.info("df has rows:" + df.count())
      df.foreach(
        msg => {
          try{
            logger.info("start executing:" + msg + ", executing thread:"+Thread.currentThread().getId)
            //Thread.sleep(1000)
          }
          catch {
            case e: Exception => logger.error("processing message error, msg:"+msg, e)
          }
        })
      df.show()
      logger.info("finish, starting to commit")
      streaming.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })


    ssc.start()


    try {
      ssc.awaitTermination()
      logger.info("*** streaming terminated")
    } catch {
      case e: Exception => {
        logger.error("*** streaming exception caught in monitor thread")
      }
    }


  }

}
