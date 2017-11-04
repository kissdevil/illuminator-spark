package com.steve.datanorm.processor

import com.steve.datanorm.entity.ItemBrand
import org.apache.spark.sql.Row

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait BrandParseProcessor extends Processor[Row, ItemBrand]{

  override def process(row: Row): ItemBrand = {
    val categoryCode = row.getAs[Long]("displayCategoryCode")
    new ItemBrand(row.getAs[String]("itemId").toLong,
      200,
      row.getAs[String]("productId").toLong,
      row.getAs[Long]("displayCategoryCode"),
      row.getAs[String]("brand"))
  }

}
