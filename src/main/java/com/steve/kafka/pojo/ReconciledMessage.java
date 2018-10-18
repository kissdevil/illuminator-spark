package com.steve.kafka.pojo;

import java.io.Serializable;

/**
 * @author stevexu
 * @since 10/16/18
 */
public class ReconciledMessage implements Serializable {

    private Long itemId;

    private String title;

    private String originalBrand;

    private String originalCategories;

    private String manufacturer;

    private long timestamp;

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

    public ReconciledMessage(){

    }

    public ReconciledMessage(Long itemId, String title, String originalBrand, String originalCategories, String manufacturer, long timestamp) {
        this.itemId = itemId;
        this.title = title;
        this.originalBrand = originalBrand;
        this.originalCategories = originalCategories;
        this.manufacturer = manufacturer;
        this.timestamp = timestamp;
    }

    public ReconciledMessage(Long itemId, String title, String originalBrand, String originalCategories, String manufacturer) {
        this.itemId = itemId;
        this.title = title;
        this.originalBrand = originalBrand;
        this.originalCategories = originalCategories;
        this.manufacturer = manufacturer;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReconciledMessage)) return false;

        ReconciledMessage that = (ReconciledMessage) o;

        if (itemId != null ? !itemId.equals(that.itemId) : that.itemId != null) return false;
        if (title != null ? !title.equals(that.title) : that.title != null) return false;
        if (originalBrand != null ? !originalBrand.equals(that.originalBrand) : that.originalBrand != null) return false;
        if (originalCategories != null ? !originalCategories.equals(that.originalCategories) : that.originalCategories != null)
            return false;
        return manufacturer != null ? manufacturer.equals(that.manufacturer) : that.manufacturer == null;

    }

    @Override
    public int hashCode() {
        int result = itemId != null ? itemId.hashCode() : 0;
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (originalBrand != null ? originalBrand.hashCode() : 0);
        result = 31 * result + (originalCategories != null ? originalCategories.hashCode() : 0);
        result = 31 * result + (manufacturer != null ? manufacturer.hashCode() : 0);
        return result;
    }
}
