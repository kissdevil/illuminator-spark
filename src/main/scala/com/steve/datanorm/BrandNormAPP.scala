package com.steve.datanorm

import com.steve.datanorm.flow.BrandNormFlow
import com.steve.datanorm.reader.BrandMosesReader
import com.steve.datanorm.service.SourceLoaderService
import com.typesafe.scalalogging.Logger

/**
  * @author stevexu
  * @Since 11/4/17
  */
object BrandNormApp extends App{

  private[this] val logger = Logger(this.getClass)

  object BrandNormSourceLoader extends SourceLoaderService with BrandMosesReader

  def main(args: Array[String]) {

    logger.info("=== start brand extraction job ===")

    BrandNormFlow.process()
  }

}
