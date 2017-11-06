package com.steve.datanorm.service

import com.steve.datanorm.writer.DataSetWriter
import org.apache.spark.sql._

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait WriterService {

  this: DataSetWriter => def write(dataFrame: DataFrame): Unit = write(dataFrame)
}

