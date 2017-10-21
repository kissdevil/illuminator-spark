package com.steve.illuminator.jobs

import com.steve.illuminator.common.CommonCassandraJob
import com.steve.illuminator.processor.ItemProcessor

object InitializeJob extends CommonCassandraJob {
  def main(args: Array[String]) {
    println("== Initialize Job ==")

    init(args)

    ItemProcessor.process(sc, ss)
  }
}
