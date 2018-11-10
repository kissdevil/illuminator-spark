package com.steve.streaming

import com.fasterxml.jackson.annotation.JsonInclude

/**
  * @author stevexu
  * @since 11/9/18
  */
@JsonInclude(JsonInclude.Include.NON_NULL)
case class CqiBrandSourceStreamingUpdate(itemId: Long, productId: Long,
                                     originalBrand: String, productName: String,
                                     manufacturer: String, originalCategoryCodes: String,
                                     cqiLevelFourCategoryCode: Long, cqiLevelThreeCategoryCode: Long,
                                     cqiLevelTwoCategoryCode: Long, cqiLevelOneCategoryCode: Long,
                                     preCqiBrand: Long, timeStamp: Long)

