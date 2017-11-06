package com.steve.datanorm.service

import com.steve.datanorm.reader.DataSetReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait SourceLoaderService {

  this: DataSetReader[DataFrame] =>
  def load(): DataFrame = read()

}
