package com.steve.streaming

import com.fasterxml.jackson.annotation.JsonInclude

/**
  * @author stevexu
  * @since 10/15/18
  */
@JsonInclude(JsonInclude.Include.NON_NULL)
case class ReconciledMessage(itemId: Int, title: String, originalBrand: String,
                             originalCategories: String, manufacturer: String)
