package com.steve.datanorm.service

import com.steve.datanorm.writer.DataNormWriter
import org.apache.spark.sql._

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait WriterService {

  this: DataNormWriter => def write(dataFrame: DataFrame): Unit = write(dataFrame)
}

