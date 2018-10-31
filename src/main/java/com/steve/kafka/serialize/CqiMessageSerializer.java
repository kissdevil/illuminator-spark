package com.steve.kafka.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.steve.kafka.pojo.CqiMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * @author stevexu
 * @since 10/30/18
 */
public class CqiMessageSerializer implements Serializer<CqiMessage> {

    private static final String ENCODING = "UTF8";

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map map, boolean b) {
        assert b;
    }

    @Override
    public byte[] serialize(String s, CqiMessage data) {
        if (data == null) {
            return null;
        }
        try {
            final String json = mapper.writeValueAsString(data);
            return json.getBytes(ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + ENCODING, e);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error when json processing EventKey to byte[]", e);
        }
    }

    @Override
    public void close() {

    }

}
