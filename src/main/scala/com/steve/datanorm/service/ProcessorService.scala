package com.steve.datanorm.service

import com.steve.datanorm.entity.ItemBrand
import com.steve.datanorm.processor.Processor
import org.apache.spark.sql._

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait ProcessorService {

  this: Processor[Row, ItemBrand] => def process(row: Row): ItemBrand = process(row)
}

