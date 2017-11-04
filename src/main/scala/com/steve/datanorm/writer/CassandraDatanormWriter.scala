package com.steve.datanorm.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait CassandraDatanormWriter extends DatanormWriter{

  override def write(dataFrame: DataFrame): Unit = {
    dataFrame.
        write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "buyboxtest", "table" -> "item_brand"))
        .mode(SaveMode.Append)
        .save()
  }

}
