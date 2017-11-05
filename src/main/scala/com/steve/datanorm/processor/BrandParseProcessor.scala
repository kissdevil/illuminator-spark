package com.steve.datanorm.processor

import com.steve.datanorm.cache.dictionary.RedisRepository
import com.steve.datanorm.cache.dictionary.entity.RedisCategoryBrand
import com.steve.datanorm.entity.ItemBrand
import org.apache.commons.lang.StringUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait BrandParseProcessor extends Processor[Row, ItemBrand] {

  override def process(row: Row): ItemBrand = {
    println("category:"+row.getAs[Long]("displayCategoryCode")+
        " itemid:"+row.getAs[String]("itemId").toLong +" on partition:"+TaskContext.getPartitionId())
    val categoryCode = row.getAs[Long]("displayCategoryCode")
    val categoryBrand = RedisRepository.getCategoryBrand(categoryCode)
    new ItemBrand(row.getAs[String]("itemId").toLong,
      extractBrand(categoryBrand, row.getAs[String]("brand")),
      row.getAs[String]("productId").toLong,
      row.getAs[Long]("displayCategoryCode"),
      row.getAs[String]("brand"))

  }

  def extractBrand(categoryBrand: RedisCategoryBrand, originalBrand: String): Long = {
    if(categoryBrand == null || StringUtils.isEmpty(originalBrand) || categoryBrand.id == -1){
      return 0
    }
    else{
      for (brand <- categoryBrand.brands) {
        if (brand.name.indexOf(originalBrand) > -1) return brand.id
        if (brand.synonyms!=null && brand.synonyms.exists(s => s.indexOf(originalBrand) > -1)) return brand.id
      }
    }
    0
  }

}
