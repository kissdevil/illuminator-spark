package com.steve.deserializer;

import com.steve.streaming.JsonUtil;
import com.steve.streaming.ReconciledMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author stevexu
 * @since 10/16/18
 */

public class ReconciledMessageDeserializer implements Deserializer<ReconciledMessage> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        assert (isKey);
    }

    @Override
    public ReconciledMessage deserialize(String topic, byte[] data) {
        ReconciledMessage reconciledMessage = null;
        try {
            reconciledMessage = JsonUtil.objectMapper().readValue(data, ReconciledMessage.class);
        } catch (Exception ex) {
            throw new SerializationException("Error when json processing byte[] to EventKey", ex);
        }
        return reconciledMessage;
    }

    @Override
    public void close() {

    }
}
