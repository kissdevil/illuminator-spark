package com.steve.kafka.serialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.steve.kafka.pojo.ReconciledMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * @author stevexu
 * @since 10/16/18
 */
public class ReconciledMessageDeSerializer implements Deserializer<ReconciledMessage> {

    private static final String ENCODING = "UTF8";

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        assert isKey;
    }

    @Override
    public ReconciledMessage deserialize(String s, byte[] bytes) {
        ReconciledMessage reconciledMessage = null;
        try {
            reconciledMessage = mapper.readValue(bytes, ReconciledMessage.class);
        } catch (IOException e) {
            throw new SerializationException("Error when json processing byte[] to EventKey", e);
        }
        return reconciledMessage;
    }


    @Override
    public void close() {
    }
}
