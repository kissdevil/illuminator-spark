package com.steve.illuminator

import org.apache.commons.lang3.StringUtils


/**
  * @author stevexu
  * @since 1/8/18
  */
object ScalaCollectionTest {

  def main(args: Array[String]): Unit = {
    val categoryCodes = Seq("69187","81103","28839")
    val dedupSecondLevelCatCodes = scala.collection.mutable.Set[String]()
    categoryCodes.foreach(
      categoryCode => {
        println(categoryCode)
        val secondLevelCat = "100"
        if(StringUtils.isNotEmpty(secondLevelCat) && secondLevelCat.toInt > 0){
          dedupSecondLevelCatCodes += secondLevelCat
        }
      }
    )
    dedupSecondLevelCatCodes.toArray

    val noRefinedCount = 10000L.toDouble
    val refinedCount = 60000L.toDouble
    println((noRefinedCount*100/(noRefinedCount+refinedCount))
        .formatted("%.2f"))
  }

}
