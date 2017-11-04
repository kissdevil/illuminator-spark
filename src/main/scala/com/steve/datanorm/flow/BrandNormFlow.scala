package com.steve.datanorm.flow

import com.steve.datanorm.processor.{BrandParseProcessor, Processor}
import com.steve.datanorm.reader.DatanormMosesReader
import com.steve.datanorm.service.{ProcessorService, SourceLoaderService, WriterService}
import com.steve.datanorm.writer.CassandraDatanormWriter
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author stevexu
  * @Since 11/4/17
  */
object BrandNormFlow{

  object BrandNormLoaderService extends SourceLoaderService with DatanormMosesReader

  object BrandParseService extends ProcessorService with BrandParseProcessor

  object BrandWriterService extends WriterService with CassandraDatanormWriter

  def process(session: SparkSession): Unit = {
      import session.implicits._
      val source = BrandNormLoaderService.read(session)
      val parsed = source.rdd.map(BrandParseService.process(_)).toDF("itemid", "brandid",
        "productid", "categorycode", "originalbrand")
      BrandWriterService.write(parsed)
  }

}
