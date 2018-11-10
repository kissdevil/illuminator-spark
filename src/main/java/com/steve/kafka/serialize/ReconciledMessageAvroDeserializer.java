package com.steve.kafka.serialize;

import com.coupang.catalog.message.avro.AvroSpecificRecordSerializer;
import com.coupang.catalog.message.demeter.source.update.v1.CqiBrandSourceUpdateValue;
import com.steve.streaming.CqiBrandSourceStreamingUpdate;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author stevexu
 * @since 11/9/18
 */
public class ReconciledMessageAvroDeserializer implements Deserializer<CqiBrandSourceStreamingUpdate> {

    private AvroSpecificRecordSerializer serializer = new AvroSpecificRecordSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        assert isKey;
    }

    @Override
    public CqiBrandSourceStreamingUpdate deserialize(String topic, byte[] data) {
        CqiBrandSourceUpdateValue cqiBrandSourceUpdateValue
                = serializer.deserialize(data, CqiBrandSourceUpdateValue.class);
        CqiBrandSourceStreamingUpdate cqiBrandSourceStreamingUpdate =
                new CqiBrandSourceStreamingUpdate(cqiBrandSourceUpdateValue.getItemId(),
                cqiBrandSourceUpdateValue.getProductId(), cqiBrandSourceUpdateValue.getOriginalBrand(),
                cqiBrandSourceUpdateValue.getProductName(), cqiBrandSourceUpdateValue.getManufacturer(),
                cqiBrandSourceUpdateValue.getOriginalCategoryCodes(), cqiBrandSourceUpdateValue.getCqiLevelFourCategoryCode(),
                cqiBrandSourceUpdateValue.getCqiLevelThreeCategoryCode(), cqiBrandSourceUpdateValue.getCqiLevelTwoCategoryCode(),
                cqiBrandSourceUpdateValue.getCqiLevelOneCategoryCode(), cqiBrandSourceUpdateValue.getPreCqiBrand(),
                cqiBrandSourceUpdateValue.getTimeStamp());
        return  cqiBrandSourceStreamingUpdate;
    }

    @Override
    public void close() {

    }
}
