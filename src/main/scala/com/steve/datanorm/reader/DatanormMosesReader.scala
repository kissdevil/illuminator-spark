package com.steve.datanorm.reader

import com.steve.datanorm.GlobalContext
import com.steve.datanorm.dataset.HiveTable
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, _}

/**
  * @author stevexu
  * @Since 11/4/17
  */
trait DataNormMosesReader extends DataNormReader[DataFrame] {

  override def read(): DataFrame = {

    val session = GlobalContext.session
    import session.implicits._

    def loadReconciledItem: DataFrame = {
      val reconciled_item = HiveTable.load(HiveTable.MOSES_RECONCILED_ITEM).filter($"locale" === "ko_KR")

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
      parsed_reconciled_item
    }

    val parsed_reconciled_item: DataFrame = loadReconciledItem

    val displaycode = HiveTable.load(HiveTable.MOSES_DISPLAY_CATEGORY_CODE)
        .filter($"displayItemType" === "PRODUCT" && $"deleted" === 0)

    val items = HiveTable.load(HiveTable.MOSES_ITEM)
        .filter($"valid" === 1)

    val brandFinalSource = parsed_reconciled_item.join(items, parsed_reconciled_item("itemId") === items("itemId"), "inner").
        join(displaycode, items("productId") === displaycode("displayItemId"), "inner").orderBy( displaycode("displayCategoryCode"),items("productId"))
        .select(parsed_reconciled_item("itemId"), parsed_reconciled_item("productName"), parsed_reconciled_item("brand"),
      items("productId"), displaycode("displayCategoryCode")
    )

    brandFinalSource
  }


}
