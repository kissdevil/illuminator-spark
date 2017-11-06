package com.steve.datanorm.processor

import com.steve.datanorm.cache.dictionary.RedisRepository
import com.steve.datanorm.cache.dictionary.entity.RedisCategoryBrand
import com.steve.datanorm.entity.ItemBrand
import org.apache.commons.lang.StringUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait BrandParseProcessor extends Processor[Row, ItemBrand] {

  override def process(row: Row): ItemBrand = {
    println("category:" + row.getAs[String]("category") +
        " productid:" + row.getAs[String]("productId").toLong +
        " itemid:" + row.getAs[String]("itemId").toLong + " on partition:" + TaskContext.getPartitionId())
    val categoryCodes = row.getAs[String]("category").split(",")
    val categoryBrands = RedisRepository.getCategoryBrand(categoryCodes)
    new ItemBrand(row.getAs[String]("itemId").toLong,
      extractBrand(categoryBrands, row.getAs[String]("brand")),
      row.getAs[String]("productId").toLong,
      row.getAs[String]("category"),
      row.getAs[String]("brand"))
  }

  def extractBrand(categoryBrands: Array[RedisCategoryBrand], originalBrand: String): Long = {
    if (StringUtils.isEmpty(originalBrand)) return 0
    categoryBrands.foreach(categoryBrand => {
      if (categoryBrand != null && categoryBrand.id > -1) {
        for (brand <- categoryBrand.brands) {
          if (brand.name.indexOf(originalBrand) > -1) return brand.id
          if (brand.synonyms != null && brand.synonyms.exists(s => s.indexOf(originalBrand) > -1)) return brand.id
        }
      }
    }
    )
    0
  }

}
