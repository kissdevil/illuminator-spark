package com.steve.kafka.producer;

import com.coupang.catalog.message.demeter.source.update.v1.CqiBrandSourceUpdateValue;
import com.steve.kafka.pojo.ReconciledBrandMessage;
import com.steve.kafka.serialize.ReconciledMessageAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * Created by stevexu on 1/16/17.
 */
public class ReconciledMessageFakeProducer {

    static Producer<String, CqiBrandSourceUpdateValue> producer;

    private static final Logger logger = LoggerFactory.getLogger(ReconciledMessageFakeProducer.class);

    public static void main(String[] args) throws Exception {
        InputStream input = ReconciledMessageFakeProducer.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(input);

        initProducer(properties);
        sendBatch(producer, properties.getProperty("brandstreamingtopic"));
    }

    public static void initProducer(Properties properties) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("brandstreaminghosts"));
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ReconciledMessageAvroSerializer.class.getName());

        KafkaProducer kafkaProducer = new KafkaProducer<String, ReconciledBrandMessage>(props);
        producer = kafkaProducer;
    }

    public static void sendBatch(Producer<String, CqiBrandSourceUpdateValue> producer, String topic) throws InterruptedException {
        for (int round = 1; round <= 100; round++) {
            for (int i = 1; i <= 50; i++) {
                //Long itemId = Long.valueOf(round - 1) * 10 + i;
                Long itemId = Long.valueOf(round - 1) * 10;
                ProducerRecord<String, CqiBrandSourceUpdateValue> message = new ProducerRecord<>(topic, String.valueOf(itemId),
                              new CqiBrandSourceUpdateValue(itemId, 1L, "sk2", "SK-II sk ii 중반 기적의 본질, 1.7 온스, 단일상품", "other manufacturer",
                                                    "56112",0L, 0L, 0L, 0L, 11236L, new Date().getTime()));
                producer.send(message, (RecordMetadata recordMetadata, Exception e) -> {
                    if (e != null) {
                        logger.error("error while send to kafka, itemid:" + message.value().getItemId(), e);
                    }
                });
                ProducerRecord<String, CqiBrandSourceUpdateValue> message2 = new ProducerRecord<>(topic, String.valueOf(itemId),
                         new CqiBrandSourceUpdateValue(itemId, 2L, "helloKitty", "헬로키티 욕실화 얼굴몰드형 랜덤 발송", "other manufacturer",
                                                     "69182",0L, 0L, 0L, 0L, 25526L, new Date().getTime()));
                producer.send(message2, (RecordMetadata recordMetadata, Exception e) -> {
                    if (e != null) {
                        logger.error("error while send to kafka, itemid:" + message2.value().getItemId(), e);
                    }
                });
            }
            //Thread.sleep(1000*5);
        }
        producer.close();
    }

}
