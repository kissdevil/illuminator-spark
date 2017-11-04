package com.steve.datanorm

import com.steve.datanorm.flow.BrandNormFlow
import com.steve.datanorm.reader.DataNormMosesReader
import com.steve.datanorm.service.SourceLoaderService

/**
  * @author stevexu
  * @Since 11/4/17
  */
object BrandNormApp extends App{

  object BrandNormSourceLoader extends SourceLoaderService with DataNormMosesReader

  def main(args: Array[String]) {

    println("== start brand extraction job ==")

    BrandNormFlow.process()
  }

}
