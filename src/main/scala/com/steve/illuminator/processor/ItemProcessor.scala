package com.steve.illuminator.processor

import org.apache.spark.{SparkContext, TaskContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.SqlRowWriter
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.Logger
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, _}
import org.apache.spark.sql._

import org.apache.spark.sql.types._;

/**
  * @author stevexu
  * @Since 10/21/17
  */
object ItemProcessor {

  private[this] val logger = Logger(this.getClass)

  def process(sc: SparkContext, ss: SparkSession): Unit = {

    import ss.implicits._

    val displaycode = ss.read.table("display_category_display_item_rel_moses")
        .filter($"displayItemType" === "PRODUCT" && $"deleted" === 0)

    val items = ss.read.table("items_moses")
        .filter($"valid" === 1)

    val reconciled_item = ss.read.table("reconciled_item_1_moses").filter($"locale" === "ko_KR")

    val productName = Integer.MAX_VALUE -1
    val categoryId = Integer.MAX_VALUE -2
    val manufacturer = Integer.MAX_VALUE - 3
    val brand = Integer.MAX_VALUE -4
    val searchTag = Integer.MAX_VALUE -5
    val barcode = Integer.MAX_VALUE -6
    val modelNo = Integer.MAX_VALUE -7
    val adultSold = Integer.MAX_VALUE -8

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
        .drop($"value").groupBy($"itemId",$"locale")
        .agg(
          max($"productName").as("productName")
          , max($"brand").as("brand")
        )

    parsed_reconciled_item.show()

    parsed_reconciled_item.join(items, parsed_reconciled_item("itemId") === items("itemId"), "inner").
        join(displaycode, items("productId") === displaycode("displayItemId"), "inner").select(
      parsed_reconciled_item("itemid"), parsed_reconciled_item("productName"), parsed_reconciled_item("brand"),
      items("productid"), displaycode("displaycategorycode")
    ).show()






    /* val rdd = sc.cassandraTable("buyboxtest", "vendor_items")
     rdd.cache
     println(rdd.count())*/

    /*val vendor_items = ss.read.table("buyboxtest.vendor_items").filter("vendorid = 'A00010029'");

    val vendors = ss.read.table("buyboxtest.vendors")

    vendor_items.join(vendors, vendor_items("vendorid") === vendors("vendorid"), "inner").
        select(
          vendor_items("vendoritemid"),
          vendor_items("itemid"),
          vendors("vendorid"),
          vendors("banned")
        ).repartition(10).foreachPartition(iterator => {
      val list = iterator.toList
      list.sliding(200, 200).foreach(block => {
         println(block)
      })
    })*/


    /*val vendor_items = ss.read.format("org.apache.spark.sql.cassandra").
        options(Map("keyspace" -> "buyboxtest", "table" -> "vendor_items")).load().limit(1000);


    val vendors = ss.read.format("org.apache.spark.sql.cassandra").
        options(Map("keyspace" -> "buyboxtest", "table" -> "vendors")).load();

    vendor_items.join(vendors, vendor_items("vendorid") === vendors("vendorid"), "inner").
        select(
          vendor_items("vendoritemid"),
          vendor_items("itemid"),
          vendors("vendorid"),
          vendors("banned")
        ).repartition(10).foreachPartition(iterator => {
      val list = iterator.toList
      list.sliding(200, 200).foreach(block => {
        {
          block
              .foreach(row => printVendorItem(row))

        }
      })
    })
*/

    /*val rows: RDD[Row] = vendor_items.join(vendors, vendor_items("vendorid") === vendors("vendorid"), "inner").
        select(
          vendor_items("vendoritemid"),
          vendor_items("itemid"),
          vendors("vendorid"),
          vendors("banned")
        ).rdd
    implicit val rowWriter = SqlRowWriter.Factory
    rows.saveToCassandra("buyboxtest", "vendor_items_detail", SomeColumns("vendoritemid", "itemid","vendorid","banned"))
*/

    /*vendor_items.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "buyboxtest", "table" -> "vendor_items_copy"))
        .mode(SaveMode.Append)
        .save()*/

    /*vendor_items.join(vendors, vendor_items("vendorid") === vendors("vendorid"), "inner").
        select(
          vendor_items("vendoritemid"),
          vendor_items("itemid"),
          vendors("vendorid"),
          vendors("banned")
        )
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "buyboxtest", "table" -> "vendor_items_detail"))
        .mode(SaveMode.Append)
        .save()*/

  }

  def printVendorItem(row: Row): Unit = {
      println(row.getAs[Long]("vendoritemid").toString, row.getAs[Long]("itemid").toString,
        row.getAs[Long]("vendorid").toString, row.getAs[Long]("banned").toString)
      println ("subline====" + TaskContext.get.partitionId())
      Thread.sleep(1000)
  }


}
