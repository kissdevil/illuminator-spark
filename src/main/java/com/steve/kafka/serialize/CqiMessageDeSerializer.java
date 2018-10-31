package com.steve.kafka.serialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.steve.kafka.pojo.CqiMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author stevexu
 * @since 10/30/18
 */
public class CqiMessageDeSerializer implements Deserializer<CqiMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ReconciledMessageDeSerializer.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        assert isKey;
    }

    @Override
    public CqiMessage deserialize(String s, byte[] bytes) {
        CqiMessage cqiMessage = null;
        try {
            cqiMessage = mapper.readValue(bytes, CqiMessage.class);
        } catch (IOException e) {
            logger.error("Error when json processing byte[] to EventKey", e);
        }
        return cqiMessage;
    }


    @Override
    public void close() {
    }
}
