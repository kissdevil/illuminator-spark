package com.steve.datanorm.processor

import com.steve.datanorm.entity.ItemBrand
import org.apache.spark.sql.Row

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait Processor[A, B] {

  def process(row: A): B

}
