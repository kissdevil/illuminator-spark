package com.steve.datanorm.service

import com.steve.datanorm.reader.DatanormReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait SourceLoaderService {

  this: DatanormReader[DataFrame] =>
  def load(session: SparkSession): DataFrame = read(session)

}
