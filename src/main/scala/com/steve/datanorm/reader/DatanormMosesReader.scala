package com.steve.datanorm.reader

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, _}

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait DatanormMosesReader extends DatanormReader[DataFrame] {

  override def read(session: SparkSession): DataFrame ={
    import session.implicits._

    val displaycode = session.read.table("display_category_display_item_rel_moses")
        .filter($"displayItemType" === "PRODUCT" && $"deleted" === 0)

    val items = session.read.table("items_moses")
        .filter($"valid" === 1)

    val reconciled_item = session.read.table("reconciled_item_1_moses").filter($"locale" === "ko_KR")

    val productName = Integer.MAX_VALUE - 1
    val categoryId = Integer.MAX_VALUE - 2
    val manufacturer = Integer.MAX_VALUE - 3
    val brand = Integer.MAX_VALUE - 4
    val searchTag = Integer.MAX_VALUE - 5
    val barcode = Integer.MAX_VALUE - 6
    val modelNo = Integer.MAX_VALUE - 7
    val adultSold = Integer.MAX_VALUE - 8

    val reconciledAttributes = new StructType()
        .add("attributeValue", StringType, nullable = true)
        .add("attributeValueWithUnit", StringType, nullable = true)

    val internalReconciledItem = StructType(Seq(
      StructField("version", StringType, nullable = true),
      StructField("itemId", LongType, nullable = true),
      StructField("locale", StringType, nullable = true),
      StructField("mainImage", StringType, nullable = true),
      StructField("detailImages", ArrayType(StringType), nullable = true),
      StructField("reconciledAttributes", MapType(StringType, reconciledAttributes), nullable = true)
    ))

    val parsed_reconciled_item = reconciled_item
        .select($"itemId", $"locale", from_json(reconciled_item("internalReconciledItem"), internalReconciledItem).as("internalReconciliation"))
        .select($"itemId", $"locale", explode($"internalReconciliation.reconciledAttributes"))
        .filter($"key" === brand || $"key" === productName)
        .withColumn("productName", when($"key" === productName, $"value.attributeValue"))
        .withColumn("brand", when($"key" === brand, $"value.attributeValue"))
        .drop($"value").groupBy($"itemId", $"locale")
        .agg(
          max($"productName").as("productName")
          , max($"brand").as("brand")
        )

    val brandFinalSource = parsed_reconciled_item.join(items, parsed_reconciled_item("itemId") === items("itemId"), "inner").
        join(displaycode, items("productId") === displaycode("displayItemId"), "inner").orderBy(displaycode("displayCategoryCode")).select(
      parsed_reconciled_item("itemId"), parsed_reconciled_item("productName"), parsed_reconciled_item("brand"),
      items("productId"), displaycode("displayCategoryCode")
    )

    brandFinalSource
  }

}
