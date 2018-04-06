package com.steve.illuminator

import scala.collection.immutable.ListMap

/**
  * @author stevexu
  * @since 1/11/18
  */
object CategoryGroup {

  val arrayItemMapping = Seq[(Long, Array[String])]((1001, Array("62589","62355")), (1002, Array("62355")), (1003, Array("62589"))
   , (1004, Array("62587","62355")), (1005, Array("62587")), (1006, Array("62589")), (1007, Array("62589","62355","62587")),
    (1008, Array("62355")), (1009, Array("62355")), (1010, Array("62587")), (1011, Array("62355")), (1012, Array("62355")),  (1013, Array("62587")),
    (1014, Array("62346")), (1015, Array("62346")), (1016, Array("62348")), (1017, Array("62348")), (1018, Array("62350")), (1019, Array("62352")),
    (1020, Array("62358")))

  def main(args: Array[String]): Unit = {
    val group = arrayItemMapping.map(toSingleCategoryMapping).flatten.groupBy(_._2).map{ case (k,v) => (k,v.map(_._1))}
    println(group)
    val sort = ListMap(group.toSeq.sortWith(_._2.length > _._2.length):_*)
    println(sort)
    val filterHighCountSort = sort.filter(_._2.length >= 4)
    println(filterHighCountSort)
    val filterTopSort = sort.take(6)
    println(filterTopSort)
    val filterSortMerge = filterHighCountSort ++ filterTopSort
    println(filterSortMerge)
    val shuffled = filterSortMerge.map(randomGetFromEachGroup)
    println(shuffled)

    println(new java.util.Date(1518348537866L))
  }

  def randomGetFromEachGroup(entry:(String, Seq[Long])): (String, String) = {
    (entry._1, scala.util.Random.shuffle(entry._2).take(10) mkString ",")
  }

  def toSingleCategoryMapping(multiple: (Long, Array[String])): Array[(Long,String)] = {
    multiple._2.map(s=> (multiple._1,s))
  }





}
