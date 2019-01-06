package com.steve.streaming

import com.fasterxml.jackson.annotation.JsonInclude

/**
  * @author stevexu
  * @since 10/15/18
  */
@JsonInclude(JsonInclude.Include.NON_NULL)
case class ReconciledBrandMessage(itemId: Long, productId:Long, title: String, originalBrand: String,
                                  originalCategories: String,
                                  predictCategoryDepth4: Long, predictCategoryDepth3: Long,
                                  predictCategoryDepth2: Long, predictCategoryDepth1: Long,
                                  manufacturer: String, nerBrand: String,
                                  prevCqiBrandId: Long,
                                  timestamp: Long, txId: String, var retryAttempts: Int)
