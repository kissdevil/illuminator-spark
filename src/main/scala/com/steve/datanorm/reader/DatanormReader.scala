package com.steve.datanorm.reader

import org.apache.spark.sql.SparkSession

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait DataNormReader[A]{

  def read(): A

}
