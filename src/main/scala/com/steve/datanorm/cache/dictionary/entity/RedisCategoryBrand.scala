package com.steve.datanorm.cache.dictionary.entity

import com.fasterxml.jackson.annotation.JsonInclude

/**
  * @author stevexu
  * @Since 11/5/17
  */
@JsonInclude(JsonInclude.Include.NON_NULL)
case class RedisCategoryBrand(id: Int, name: String, brands: Seq[RedisBrand])
