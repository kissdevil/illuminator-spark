package com.steve.streaming

import com.steve.kafka.pojo.CqiMessage
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * @author stevexu
  * @since 10/30/18
  */
class KafkaSink(createProducer: () => KafkaProducer[String, CqiMessage]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, key: String, value: CqiMessage): Unit = producer.send(new ProducerRecord(topic, key, value))

}

object KafkaSink {

  private val logger: Logger = LoggerFactory.getLogger(KafkaSink.getClass)

  def apply(config: collection.mutable.Map[String, Object]): KafkaSink = {
    val f = () => {
      logger.info("initializing kafka producer")
      val producer = new KafkaProducer[String, CqiMessage](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}