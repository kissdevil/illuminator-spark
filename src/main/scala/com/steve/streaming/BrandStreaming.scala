package com.steve.streaming

import java.util.Arrays

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions


/**
  * @author stevexu
  * @since 10/15/18
  */
object BrandStreaming {

  def main(args: Array[String]): Unit = {
    val kafkaParams = collection.mutable.Map[String, Object]()
    kafkaParams += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095")
    kafkaParams += (ConsumerConfig.GROUP_ID_CONFIG -> "brandstreaming")
    kafkaParams += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
    kafkaParams += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "1000")
    kafkaParams += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[com.steve.deserializer.ReconciledMessageDeserializer])

    val conf = new SparkConf().setAppName("BrandStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))

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

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
   /* kafkaStream.foreachRDD(r => {
      r.foreach(s => println(s))
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("*** partition size = " + a.size))
      }
      println("finish, starting to commit")
      val offsetRanges = r.asInstanceOf[HasOffsetRanges].offsetRanges
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
*/
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

      //print("df has"+df.g)
      df.show()
      println("finish, starting to commit")
      streaming.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()


    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }


  }

}
