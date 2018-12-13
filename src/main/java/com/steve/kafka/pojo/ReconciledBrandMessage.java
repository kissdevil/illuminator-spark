package com.steve.kafka.pojo;

import java.io.Serializable;

/**
 * @author stevexu
 * @since 10/16/18
 */
public class ReconciledBrandMessage implements Serializable {

    private Long itemId;

    private Long productId;

    private Long prevCqiBrandId;

    private String title;

    private String originalBrand;

    private String originalCategories;

    private String manufacturer;

    private String txId;

    private long timestamp;

    private Long predictCategoryDepth4;

    private Long predictCategoryDepth3;

    private Long predictCategoryDepth2;

    private Long predictCategoryDepth1;

    private String nerBrand;

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getOriginalBrand() {
        return originalBrand;
    }

    public void setOriginalBrand(String originalBrand) {
        this.originalBrand = originalBrand;
    }

    public String getOriginalCategories() {
        return originalCategories;
    }

    public void setOriginalCategories(String originalCategories) {
        this.originalCategories = originalCategories;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public void setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public Long getPredictCategoryDepth4() {
        return predictCategoryDepth4;
    }

    public void setPredictCategoryDepth4(Long predictCategoryDepth4) {
        this.predictCategoryDepth4 = predictCategoryDepth4;
    }

    public Long getPredictCategoryDepth3() {
        return predictCategoryDepth3;
    }

    public void setPredictCategoryDepth3(Long predictCategoryDepth3) {
        this.predictCategoryDepth3 = predictCategoryDepth3;
    }

    public Long getPredictCategoryDepth2() {
        return predictCategoryDepth2;
    }

    public void setPredictCategoryDepth2(Long predictCategoryDepth2) {
        this.predictCategoryDepth2 = predictCategoryDepth2;
    }

    public Long getPredictCategoryDepth1() {
        return predictCategoryDepth1;
    }

    public void setPredictCategoryDepth1(Long predictCategoryDepth1) {
        this.predictCategoryDepth1 = predictCategoryDepth1;
    }

    public String getNerBrand() {
        return nerBrand;
    }

    public void setNerBrand(String nerBrand) {
        this.nerBrand = nerBrand;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Long getPrevCqiBrandId() {
        return prevCqiBrandId;
    }

    public void setPrevCqiBrandId(Long prevCqiBrandId) {
        this.prevCqiBrandId = prevCqiBrandId;
    }

    public ReconciledBrandMessage() {

    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ReconciledBrandMessage(Long itemId, Long productId, String title, String originalBrand, String originalCategories
            , Long predictCategoryDepth4, Long predictCategoryDepth3, Long predictCategoryDepth2, Long predictCategoryDepth1,
            String manufacturer, String nerBrand, long prevCqiBrandId, long timestamp, String txId) {
        this.itemId = itemId;
        this.productId = productId;
        this.title = title;
        this.originalBrand = originalBrand;
        this.originalCategories = originalCategories;
        this.manufacturer = manufacturer;
        this.txId = txId;
        this.timestamp = timestamp;
        this.predictCategoryDepth4 = predictCategoryDepth4;
        this.predictCategoryDepth3 = predictCategoryDepth3;
        this.predictCategoryDepth2 = predictCategoryDepth2;
        this.predictCategoryDepth1 = predictCategoryDepth1;
        this.nerBrand = nerBrand;
        this.prevCqiBrandId = prevCqiBrandId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReconciledBrandMessage)) return false;

        ReconciledBrandMessage that = (ReconciledBrandMessage) o;

        if (!itemId.equals(that.itemId)) return false;
        return txId.equals(that.txId);

    }

    @Override
    public int hashCode() {
        int result = itemId.hashCode();
        result = 31 * result + txId.hashCode();
        return result;
    }
}
