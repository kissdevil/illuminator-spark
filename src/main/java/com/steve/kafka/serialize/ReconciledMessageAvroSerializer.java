package com.steve.kafka.serialize;

import com.coupang.catalog.message.avro.AvroSpecificRecordSerializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author stevexu
 * @since 11/9/18
 */
public class ReconciledMessageAvroSerializer implements Serializer<SpecificRecordBase> {

    private static final String ENCODING = "UTF8";

    private AvroSpecificRecordSerializer serializer = new AvroSpecificRecordSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        assert isKey;
    }

    @Override
    public byte[] serialize(String topic, SpecificRecordBase data) {
        if (data == null) {
            throw new RuntimeException("Kafka serialize exception, found an empty ReconciledMessage");
        }
        return serializer.serialize(data);
    }

    @Override
    public void close() {

    }
}
