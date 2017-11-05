package com.steve.datanorm.flow

import com.steve.datanorm.GlobalContext
import com.steve.datanorm.processor.{BrandParseProcessor, Processor}
import com.steve.datanorm.reader.DataNormMosesReader
import com.steve.datanorm.service.{ProcessorService, SourceLoaderService, WriterService}
import com.steve.datanorm.writer.CassandraDatanormWriter
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author stevexu
  * @Since 11/4/17
  */
object BrandNormFlow {

  object BrandNormLoaderService extends SourceLoaderService with DataNormMosesReader

  object BrandParseService extends ProcessorService with BrandParseProcessor

  object BrandWriterService extends WriterService with CassandraDatanormWriter


  def process(): Unit = {
    val sparkSession = GlobalContext.session
    import sparkSession.implicits._

    val source = BrandNormLoaderService.read()

    val parsed = source.map(BrandParseService.process(_))

    val dataFrameToWrite = parsed.toDF("itemid", "brandid",
      "productid", "categorycode", "originalbrand")

    BrandWriterService.write(dataFrameToWrite)
  }

}
