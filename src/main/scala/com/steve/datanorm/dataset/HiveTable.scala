package com.steve.datanorm.dataset

import com.steve.datanorm.GlobalContext
import org.apache.spark.sql._

/**
  * @author stevexu
  * @Since 11/4/17
  */
object HiveTable {

  final val MOSES_RECONCILED_ITEM = "reconciled_item_1_moses"
  final val MOSES_DISPLAY_CATEGORY_CODE = "display_category_display_item_rel_moses"
  final val MOSES_ITEM = "reconciled_item_1_moses"

  def load(database: String, table: String): DataFrame = {
    GlobalContext.session.read.table(database + "." + table)
  }

  def load(table: String): DataFrame = {
     GlobalContext.session.read.table(table)
  }


}
