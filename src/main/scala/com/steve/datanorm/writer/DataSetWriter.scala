package com.steve.datanorm.writer

import org.apache.spark.sql._

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait DataSetWriter {

  def write(dataFrame: DataFrame)

}
