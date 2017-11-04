package com.steve.datanorm.service

import com.steve.datanorm.reader.DataNormReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait SourceLoaderService {

  this: DataNormReader[DataFrame] =>
  def load(): DataFrame = read()

}
