package com.steve.datanorm

import com.steve.datanorm.flow.BrandNormFlow
import com.steve.datanorm.reader.DatanormMosesReader
import com.steve.datanorm.service.SourceLoaderService

/**
  * @author stevexu
  * @Since 11/4/17
  */
object BrandNormApp extends App{

  object BrandNormSourceLoader extends SourceLoaderService with DatanormMosesReader

  def main(args: Array[String]) {

    println("== start brand extraction job ==")

    BrandNormFlow.process(session)
  }

}
