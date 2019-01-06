package com.steve.kafka.serialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.steve.kafka.pojo.ReconciledBrandMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author stevexu
 * @since 10/16/18
 */
public class ReconciledMessageDeSerializer implements Deserializer<com.steve.streaming.ReconciledBrandMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ReconciledMessageDeSerializer.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        assert isKey;
    }

    @Override
    public com.steve.streaming.ReconciledBrandMessage deserialize(String s, byte[] bytes) {
        ReconciledBrandMessage reconciledMessage = null;
        try {
            reconciledMessage = mapper.readValue(bytes, ReconciledBrandMessage.class);
        } catch (IOException e) {
            logger.error("Error when json processing byte[] to EventKey", e);
        }
        return new com.steve.streaming.ReconciledBrandMessage(reconciledMessage.getItemId(),reconciledMessage.getProductId(),
                                                              reconciledMessage.getTitle(), reconciledMessage.getOriginalBrand(),
                                                              reconciledMessage.getOriginalCategories(),
                                                              reconciledMessage.getPredictCategoryDepth4(), reconciledMessage.getPredictCategoryDepth3(),
                                                              reconciledMessage.getPredictCategoryDepth2(), reconciledMessage.getPredictCategoryDepth1(),
                                                              reconciledMessage.getManufacturer(), reconciledMessage.getNerBrand(),
                                                              reconciledMessage.getPrevCqiBrandId(),
                                                              reconciledMessage.getTimestamp(), reconciledMessage.getTxId(), 0);
    }


    @Override
    public void close() {
    }
}
