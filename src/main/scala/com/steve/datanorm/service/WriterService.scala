package com.steve.datanorm.service

import com.steve.datanorm.writer.DatanormWriter
import org.apache.spark.sql._

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait WriterService {

  this: DatanormWriter => def write(dataFrame: DataFrame): Unit = write(dataFrame)
}

