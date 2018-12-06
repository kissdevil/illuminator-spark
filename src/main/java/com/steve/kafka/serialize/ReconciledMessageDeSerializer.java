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
public class ReconciledMessageDeSerializer implements Deserializer<ReconciledBrandMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ReconciledMessageDeSerializer.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        assert isKey;
    }

    @Override
    public ReconciledBrandMessage deserialize(String s, byte[] bytes) {
        ReconciledBrandMessage reconciledMessage = null;
        try {
            reconciledMessage = mapper.readValue(bytes, ReconciledBrandMessage.class);
        } catch (IOException e) {
            logger.error("Error when json processing byte[] to EventKey", e);
        }
        return reconciledMessage;
    }


    @Override
    public void close() {
    }
}
