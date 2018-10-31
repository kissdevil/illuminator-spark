package com.steve.kafka.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

/**
 * @author stevexu
 * @since 10/30/18
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CqiMessage implements Serializable {

    private Long itemId;

    private Long cqiBrandId;


    public CqiMessage() {
    }

    public CqiMessage(Long itemId, Long cqiBrandId) {
        this.itemId = itemId;
        this.cqiBrandId = cqiBrandId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getCqiBrandId() {
        return cqiBrandId;
    }

    public void setCqiBrandId(Long cqiBrandId) {
        this.cqiBrandId = cqiBrandId;
    }
}
