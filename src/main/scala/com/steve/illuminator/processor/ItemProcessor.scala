package com.steve.illuminator.processor

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.SqlRowWriter
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD




/**
  * @author stevexu
  * @Since 10/21/17
  */
object ItemProcessor {

  private[this] val logger = Logger(this.getClass)

  def process(sc: SparkContext, ss: SparkSession): Unit = {

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


    val vendor_items = ss.read.format("org.apache.spark.sql.cassandra").
        options(Map("keyspace" -> "buyboxtest", "table" -> "vendor_items")).load().limit(1000);


    val vendors = ss.read.format("org.apache.spark.sql.cassandra").
        options(Map("keyspace" -> "buyboxtest", "table" -> "vendor_items")).load();

    /*vendor_items.join(vendors, vendor_items("vendorid") === vendors("vendorid"), "inner").
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
              .foreach(row => {
                println(row.getAs[Long]("vendoritemid").toString, row.getAs[Long]("itemid").toString,
                  row.getAs[Long]("vendorid").toString, row.getAs[Long]("banned").toString)
              })

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

    vendor_items.join(vendors, vendor_items("vendorid") === vendors("vendorid"), "inner").
        select(
          vendor_items("vendoritemid"),
          vendor_items("itemid"),
          vendors("vendorid"),
          vendors("banned")
        ).write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "buyboxtest", "table" -> "vendor_items_detail"))
        .mode(SaveMode.Append)
        .save()

  }

  def printVendorItem(row: Row): Unit = (
      println("line:" + row.getAs[Long]("vendoritemid").toString + row.getAs[Long]("itemid").toString
        , row.getAs[Long]("vendorid").toString + row.getAs[Long]("banned").toString)
      )
}
