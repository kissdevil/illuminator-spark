package com.steve.datanorm.reader

import org.apache.spark.sql.SparkSession

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait DataSetReader[A]{

  def read(): A

}
